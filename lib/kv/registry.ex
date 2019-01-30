defmodule KV.Registry do
  use GenServer

  ## Client API

  @doc """
  Start the registry
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
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
    # call makes a synchronous request
    GenServer.call(server, {:lookup, name})
  end

  @doc """
  Ensures there is a bucket associated with the given `name` on `server`.
  """
  def create(server, name) do
    # makes an asynchronous request
    GenServer.cast(server, {:create, name})
  end

  ## Server callbacks

  def init(:ok) do
    names = %{}
    refs = %{}
    {:ok, {names, refs}}
  end

  def handle_call({:lookup, name}, _from, state) do
    {names, _} = state
    {:reply, Map.fetch(names, name), state}
  end

  def handle_cast({:create, name}, {names, refs}) do
    if Map.has_key?(names, name) do
      {:noreply, {names, refs}}
    else
      {:ok, pid} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
      ref = Process.monitor(pid)
      refs = Map.put(refs, ref, name)
      names = Map.put(names, name, pid)
      {:noreply, {names, refs}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
    {name, refs} = Map.pop(refs, ref)
    names = Map.delete(names, name)
    {:noreply, {names, refs}}
  end

  # This is a catch-all that discards unknown or unexpected messages
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
