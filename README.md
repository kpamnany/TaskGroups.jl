# TaskGroups.jl

> [!NOTE]
> This is a stripped-down, but still functional version of RAI's
> TaskGroups.jl package, prepared for demonstration at JuliaCon 2025.

### Reusable worker pools for Julia

Although Julia `Task`s are reasonably lightweight, each still carries some
memory overhead, both physical and virtual. Furthermore, as the number of
runnable tasks grows, it can be seen that Julia's task scheduler is biased
towards more recently created tasks. This severely hampers responsiveness
which is particularly relevant in server applications -- we have observed
tasks that yield going unscheduled for many minutes.

A `TaskGroup` creates a limited number of Julia `Task`s and runs _jobs_ on
them, represented by `Future`s. By limiting the number of running tasks, by
default to `max(256, 4*Base.Threads.nthreads(:default))`, the overhead from
Julia's `Task`s are minimized, and a degree of control is achieved over the
order in which work is scheduled.
