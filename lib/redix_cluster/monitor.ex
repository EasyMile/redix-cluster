defmodule RedixCluster.Monitor do
  @moduledoc """
  ## RedixCluster.Monitor

  The monitor for the Redis cluster, which handles creating connections to the cluster using a pool of Redis clients.
  """

  use GenServer

  @type conn :: module | atom | pid

  @redis_cluster_hash_slots 16_384

  defmodule State do
    @moduledoc """
    ### RedixCluster.Monitor.State

    The state of the `RedixCluster.Monitor` Worker.
    """
    defstruct conn_name: nil,
              cluster_nodes: [],
              slots: [],
              slots_maps: [],
              version: 0,
              is_cluster: true
  end

  def get_env(key, default \\ nil) do
    Application.get_env(:redix_cluster_remastered, key, default)
  end

  @spec connect(conn, term) :: :ok | {:error, :connect_to_empty_nodes}
  def connect(_conn_name, []), do: {:error, :connect_to_empty_nodes}

  def connect(conn_name, cluster_nodes),
    do: GenServer.call(Module.concat(conn_name, RedixCluster.Monitor), {:connect, cluster_nodes})

  @spec refresh_mapping(conn, integer) :: :ok | {:ignore, String.t()}
  def refresh_mapping(conn_name, version),
    do:
      GenServer.call(Module.concat(conn_name, RedixCluster.Monitor), {:reload_slots_map, version})

  @spec get_slot_cache(conn) ::
          {:cluster, [binary], [integer], integer} | {:not_cluster, integer, atom}
  def get_slot_cache(conn) do
    [{:cluster_state, state}] = :ets.lookup(conn, :cluster_state)

    case state.is_cluster do
      true ->
        {:cluster, state.slots_maps, state.slots, state.version}

      false ->
        [slots_map] = state.slots_maps
        {:not_cluster, state.version, slots_map.node.pool}
    end
  end

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    {conn_name, _opts} = Keyword.pop(opts, :conn_name, RedixCluster)
    name = Module.concat(conn_name, RedixCluster.Monitor)
    GenServer.start_link(__MODULE__, conn_name, name: name)
  end

  def init(conn_name) do
    :ets.new(conn_name, [:protected, :set, :named_table, {:read_concurrency, true}])

    case get_env(:cluster_nodes, []) do
      [] -> {:ok, %State{conn_name: conn_name}}
      cluster_nodes -> {:ok, do_connect(conn_name, cluster_nodes)}
    end
  end

  def handle_call({:reload_slots_map, version}, _from, %State{version: version} = state) do
    {:reply, :ok, reload_slots_map(state)}
  end

  def handle_call({:reload_slots_map, version}, _from, %State{version: old_version} = state) do
    {:reply, {:ignore, "wrong version#{version}!=#{old_version}"}, state}
  end

  def handle_call({:connect, cluster_nodes}, _from, %State{conn_name: conn_name} = _state),
    do: {:reply, :ok, do_connect(conn_name, cluster_nodes)}

  def handle_call(request, _from, state), do: {:reply, {:ignored, request}, state}

  def handle_cast(_msg, state), do: {:noreply, state}

  def handle_info(_info, state), do: {:noreply, state}

  defp do_connect(conn_name, cluster_nodes) do
    %State{conn_name: conn_name, cluster_nodes: cluster_nodes} |> reload_slots_map
  end

  defp reload_slots_map(%State{conn_name: conn_name} = state) do
    for slots_map <- state.slots_maps, do: close_connection(slots_map)
    {is_cluster, cluster_info} = get_cluster_info(state.cluster_nodes)
    slots_maps = cluster_info |> parse_slots_maps |> connect_all_slots(conn_name)
    slots = create_slots_cache(slots_maps)

    new_state = %State{
      state
      | slots: slots,
        slots_maps: slots_maps,
        version: state.version + 1,
        is_cluster: is_cluster
    }

    true = :ets.insert(conn_name, [{:cluster_state, new_state}])
    new_state
  end

  defp close_connection(slots_map) do
    RedixCluster.Pool.stop_redis_pool(slots_map.node.pool)
  catch
    _ -> :ok
  end

  defp get_cluster_info([]), do: throw({:error, :cannot_connect_to_cluster})

  defp get_cluster_info([node | restnodes]) do
    case start_link_redix(node.host, node.port) do
      {:ok, conn} ->
        case Redix.command(conn, ~w(CLUSTER SLOTS), []) do
          {:ok, cluster_info} ->
            Redix.stop(conn)
            {true, cluster_info}

          {:error, %Redix.Error{message: "ERR unknown command 'CLUSTER'"}} ->
            cluster_info_from_single_node(node)

          {:error, %Redix.Error{message: "ERR This instance has cluster support disabled"}} ->
            cluster_info_from_single_node(node)
        end

      _ ->
        get_cluster_info(restnodes)
    end
  end

  # [[10923, 16383, ["Host1", 7000], ["SlaveHost1", 7001]],
  # [5461, 10922, ["Host2", 7000], ["SlaveHost2", 7001]],
  # [0, 5460, ["Host3", 7000], ["SlaveHost3", 7001]]]
  defp parse_slots_maps(cluster_info) do
    cluster_info
    |> Stream.with_index()
    |> Stream.map(&parse_cluster_slot/1)
    |> Enum.to_list()
  end

  defp connect_all_slots(slots_maps, cache_name) do
    for slot <- slots_maps, do: %{slot | node: connect_node(cache_name, slot.node)}
  end

  defp create_slots_cache(slots_maps) do
    for slots_map <- slots_maps do
      for index <- slots_map.start_slot..slots_map.end_slot, do: {index, slots_map.index}
    end
    |> List.flatten()
    |> List.keysort(0)
    |> Enum.map(fn {_index, index} -> index end)
  end

  def start_link_redix(host, port) do
    :erlang.process_flag(:trap_exit, true)
    result = Redix.start_link(host: host, port: port)
    :erlang.process_flag(:trap_exit, false)
    result
  end

  defp cluster_info_from_single_node(node) do
    {false, [[0, @redis_cluster_hash_slots - 1, [node.host, node.port]]]}
  end

  defp parse_cluster_slot({cluster_slot, index}) do
    [start_slot, end_slot | nodes] = cluster_slot

    %{
      index: index + 1,
      name: get_slot_name(start_slot, end_slot),
      start_slot: start_slot,
      end_slot: end_slot,
      node: parse_master_node(nodes)
    }
  end

  defp connect_node(cache_name, node) do
    case RedixCluster.Pool.new_pool(cache_name, node.host, node.port) do
      {:ok, pool_name} -> %{node | pool: pool_name}
      _ -> nil
    end
  end

  defp get_slot_name(start_slot, end_slot) do
    [start_slot, ":", end_slot]
    |> Enum.join()
    |> String.to_atom()
  end

  defp parse_master_node([[master_host, master_port | _] | _]) do
    %{host: to_charlist(master_host), port: master_port, pool: nil}
  end
end
