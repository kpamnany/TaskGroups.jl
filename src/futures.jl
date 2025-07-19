"""
A Future is a reference to the result of a computation that may not have completed
yet. Dereferencing the future causes the caller to block until the result is available.
Any exception thrown by the computation is rethrown when the future is dereferenced.

    Future{T}(func::Function)
    Future(func::Function)
    Future{T} do ; ... end
    Future() do ; ... end

Spawn a Task to compute `func()`, and construct a Future representing its result. If the
type parameter T is not provided, it is inferred from the return type of `func`.

    Future{T}()

Create a Future not tied to a computation. The caller is responsible for
doing either `future[]=v` or `setexception!(future,e)`.

    Future{T}(t::T)

Create a Future that is already populated with the result value t.  E.g.
`future = Future{Int}(3); future[]` immediately returns 3.

You should use Futures when you want to return exactly one value from a function
asynchronously. If you want to return arbitrarily many values, and allow processing
them as they arrive, consider using a `Channel{T}()`.

# Examples:
```
# Spawn a task that sleeps a bit then returns 10.
future = Future{Int}() do
    sleep(3)
    return 10
end

println("Waiting for the result")

# Dereference the future - this will block
x = future[]
```

```
# Grab some web pages asynchronously
using HTTP
f1 = Future(() -> HTTP.request("GET", "http://httpbin.org/ip"))
f2 = Future(() -> HTTP.request("GET", "http://julialang.org"))

page1 = f1[]
page2 = f2[]
```
"""
struct Future{T}
    channel::Channel{T}

    # Optionally, if fetch(fut) is called and fut is not yet available, then this
    # function will be called. This can be used to immediately compute the required
    # result in the calling task.
    fetch_action::Union{Nothing,Function}

    # Spawn a Task to compute func(), and return a Future representing its result.
    function Future{T}(func::Function; spawn=true) where {T}
        channel = Channel{T}(1; spawn=spawn) do ch
            # This code will run in a spawned Task.  If there is an exception,
            # Channel will catch and save it, to be thrown when the Future is dereferenced.
            r = func()
            put!(ch, r)
        end
        return new(channel, nothing)
    end

    # No computation attached - caller must use setindex! or setexception!. If
    # `fetch_action` is specified, then if the future is dereferenced and the result
    # is not yet available, fetch_action() will be invoked before fetching the result
    # from the channel. Note that due to race conditions, fetch_action() may be called
    # after the result is posted.
    function Future{T}(; fetch_action=nothing) where {T}
        return new(Channel{T}(1), fetch_action)
    end

    # No computation attached, just a result
    function Future{T}(t::T) where {T}
        channel = Channel{T}(1)
        put!(channel, t)
        return new(channel, nothing)
    end
end

# Infer the type T from the return-type of func()
function Future(func::Function)
    T = Base.return_types(func, Tuple{})
    @assert length(T) == 1
    return Future{T[1]}(func)
end

"""
    getindex(f::Future{T}) :: T

Get the result of the computation referenced by the future, blocking if it
has not yet completed. If the computation threw an exception, getindex()
will rethrow the exception in this thread.
"""
function Base.getindex(future::Future{T}) where {T}
    if !done(future)
        if !isa(future.fetch_action, Nothing)
            future.fetch_action(future)
        end
    end
    return fetch(future.channel)
end

# For compatibility with code that was previously implemented with Channels.
# The preferred function is getindex, so leave fetch undocumented.
function Base.fetch(future::Future{T}) where {T}
    return fetch(future.channel)
end

"""
    setindex!(f::Future{T}, value)

Set the value for a Future that was created with the default constructor
(without a specified function to evaluate).
"""
function Base.setindex!(future::Future{T}, value) where {T}
    put!(future.channel, value)
    return value
end

"""
    setexception!(future, e)

Set the exception for a Future that was created with the default constructor
(without a specified function to evaluate).
"""
function setexception!(future::Future{T}, @nospecialize(e::Exception)) where {T}
    close(future.channel, e)
    return nothing
end
function setexception!(future::Future{T}, @nospecialize(e)) where {T}
    close(future.channel, ErrorException(e))
    return nothing
end


"""
    wait_until_done_nothrow(futures::Vector{<:Future})

Waits until all futures are available, and then returns a CompositeException
if any exception occurred without throwing it.  If no exceptions occurred,
returns nothing.
"""
function wait_until_done_nothrow(futures::Vector{<:Future})
    # This follows the implementation of `sync_end` in Base.
    local c_ex
    for future in futures
        try
            fetch(future)
        catch e
            if !@isdefined(c_ex)
                c_ex = CompositeException()
            end
            push!(c_ex, e)
        end
    end
    if @isdefined(c_ex)
        return c_ex
    end
    return nothing
end

"""
    wait_until_done(futures::Vector{<:Future})

Waits until all futures are available, and then throws a CompositeException
if any exception occurred.
"""
function wait_until_done(futures::Vector{<:Future})
    c_ex = wait_until_done_nothrow(futures)
    if !isnothing(c_ex)
        throw(c_ex)
    else
        return nothing
    end
end

"""
    done(f::Future{T})::Bool

Returns true if future[] would return without blocking.
"""
function done(future::Future{T}) where {T}
    return isready(future.channel) || !isopen(future.channel)
end

export Future, setexception!, done
