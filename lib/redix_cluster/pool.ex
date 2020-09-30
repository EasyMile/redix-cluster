defmodule RedixCluster.Pool do
  @moduledoc """
  ## RedixCluster.Pool

  The pool supervisor, which uses `poolboy` to manage a pool of worker connections to Redis.
  """

  use Supervisor

  @type conn :: module | atom | pid

  @default_pool_size 10
  @default_pool_max_overflow 0

  def get_env(key, default \\ nil) do
    Application.get_env(:redix_cluster_remastered, key, default)
  end

  @spec start_link(RedixCluster.Options.t()) :: Supervisor.on_start()
  def start_link(%RedixCluster.Options{conn_name: conn_name}) do
    table_name = Module.concat(conn_name, Pool)
    :ets.new(table_name, [:set, :named_table, :public])
    Supervisor.start_link(__MODULE__, nil, name: table_name)
  end

  def init(nil) do
    children = []
    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec new_pool(conn, charlist, integer) :: {:ok, atom} | {:error, atom}
  def new_pool(conn, host, port) do
    pool_name = [conn, "-Pool-", host, ":", port] |> Enum.join() |> String.to_atom()
    table_name = Module.concat(conn, Pool)

    case Process.whereis(pool_name) do
      nil ->
        :ets.insert(table_name, {pool_name, 0})
        pool_size = get_env(:pool_size, @default_pool_size)
        pool_max_overflow = get_env(:pool_max_overflow, @default_pool_max_overflow)

        pool_args = [
          name: {:local, pool_name},
          worker_module: RedixCluster.Worker,
          size: pool_size,
          max_overflow: pool_max_overflow
        ]

        worker_args = [host: host, port: port, pool_name: pool_name]
        child_spec = :poolboy.child_spec(pool_name, pool_args, worker_args)
        {result, _} = Supervisor.start_child(table_name, child_spec)
        {result, pool_name}

      _ ->
        {:ok, pool_name}
    end
  end

  @spec register_worker_connection(atom) :: :ok
  def register_worker_connection(pool_name) do
    conn = pool_name |> Atom.to_string() |> String.split("-") |> Enum.at(0)
    table_name = Module.concat(conn, Pool)
    :ets.update_counter(table_name, pool_name, 1)
    :ok
  end

  @spec stop_redis_pool(atom) :: :ok | {:error, error}
        when error: :not_found | :simple_one_for_one | :running | :restarting
  def stop_redis_pool(pool_name) do
    conn = pool_name |> Atom.to_string() |> String.split("-") |> Enum.at(0)
    table_name = Module.concat(conn, Pool)
    Supervisor.terminate_child(table_name, pool_name)
    Supervisor.delete_child(table_name, pool_name)
  end
end
