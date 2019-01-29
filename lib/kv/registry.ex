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
    {:ok, %{}}
  end

  def handle_call({:lookup, name}, _from, names) do
    {:reply, Map.fetch(names, name), names}
  end

  def handle_cast({:create, name}, names) do
    if Map.has_key?(names, name) do
      {:noreply, names}
    else
      {:ok, bucket} = KV.Bucket.start_link([])
      {:noreply, Map.put(names, name, bucket)}
    end
  end
end