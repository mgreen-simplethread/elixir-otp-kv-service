defmodule KV.Registry do
  use GenServer

  ## Client API

  @doc """
  Start the registry
  """
  def start_link(opts) do
    # Pass ETS table name to GenServer's init
    server = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, server, opts)
  end

  @doc """
  Stops the registry
  """
  def stop(server) do
    GenServer.stop(server)
  end

  @doc """
  Looks up the bucket PID for `name` stored in `server`.

  Returns `{:ok, pid}` if bucket exists, `{:error}` otherwise.
  """
  def lookup(server, name) do
    # Lookup is done directly in ETS, without accessing the server:
    case :ets.lookup(server, name) do
      [{^name, pid}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Ensures there is a bucket associated with the given `name` on `server`.
  """
  def create(server, name) do
    # makes an asynchronous request
    GenServer.call(server, {:create, name})
  end

  ## Server callbacks

  def init(table) do
    # We replace the names map with an ETS table:
    names = :ets.new(table, [:named_table, read_concurrency: true])
    refs = %{}
    {:ok, {names, refs}}
  end

  def handle_call({:create, name}, _from, {names, refs}) do
    case lookup(names, name) do
      {:ok, pid} ->
        {:reply, pid, {names, refs}}
      :error ->
        {:ok, pid} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
        ref = Process.monitor(pid)
        refs = Map.put(refs, ref, name)
        :ets.insert(names, {name, pid})
        {:reply, pid, {names, refs}}
    end
  end

  def handle_cast({:create, name}, {names, refs}) do
    # Read and write to ETS table instead of the previous Map
    case lookup(names, name) do
      {:ok, _pid} ->
        {:noreply, {names, refs}}
      :error ->
        {:ok, pid} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
        ref = Process.monitor(pid)
        refs = Map.put(refs, ref, name)
        :ets.insert(names, {name, pid})
        {:noreply, {names, refs}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
    # Delete from the ETS table instead of the Map
    {name, refs} = Map.pop(refs, ref)
    :ets.delete(names, name)
    {:noreply, {names, refs}}
  end

  # This is a catch-all that discards unknown or unexpected messages
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
