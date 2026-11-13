# External module dependencies
from collections import deque

# Internal module dependencies
from .evaluate import Task, Program

###############################################################################
# Functions
###############################################################################
def has_cycle(nodes : list[Task]) -> bool:
    # Depth-first search from every node; a dependency found on the current
    # path closes a cycle. Searching only from nodes without dependencies
    # would miss cycles that no such node leads into.
    checked : set[Task] = set()
    for root_node in nodes:
        if root_node in checked: continue
        path : list[Task] = [root_node]
        on_path : set[Task] = {root_node}
        stack : list[list[Task]] = [list(root_node.get_deps())]
        while len(stack) != 0:
            node_deps = stack[-1]
            if len(node_deps) == 0:
                node = path.pop(-1)
                on_path.remove(node)
                checked.add(node)
                stack.pop(-1)
                continue
            # From the end: the offline terminating task depends on every
            # task, and popping from the front made its search quadratic
            node_dep = node_deps.pop()
            if node_dep in checked: continue
            if node_dep in on_path: return True
            path.append(node_dep)
            on_path.add(node_dep)
            stack.append(list(node_dep.get_deps()))
    return False

Graph = list[Task]

def get_leafs(graph : Graph):
    return list(filter(lambda node: len(node.get_deps()) == 0, graph))

def topological_order(graph : Graph) -> list[Task]:
    """The tasks of an acyclic graph, each after all of its dependencies."""
    waiting : dict[Task, int] = {
        node : len(node.get_deps()) for node in graph
    }
    result : list[Task] = get_leafs(graph)
    index = 0
    while index < len(result):
        node = result[index]
        index += 1
        for ref in node.get_refs():
            if ref not in waiting: continue
            waiting[ref] -= 1
            if waiting[ref] == 0: result.append(ref)
    return result

def propagate(graph : Graph):
    # Defensively check for cycles
    if has_cycle(graph):
        raise RuntimeError('Cycle detected in agenda!')

    # Propagate invalidity
    for dst in topological_order(graph):
        for src in dst.get_deps():
            if not src.get_valid(): dst.set_valid(False)
            if not src.get_active(): dst.set_active(False)

def _levels(
    nodes : list[int],
    deps : dict[int, set[int]],
    refs : dict[int, set[int]],
    barriers : set[int]
    ) -> list[list[int]]:
    """The nodes of an acyclic graph grouped by level, where a node's level
    is one more than the highest level of its dependencies. A barrier only
    passes its level on: it shares the level of the nodes after it rather
    than taking one of its own, which would hold them back a level."""
    waiting : dict[int, int] = { node : len(deps[node]) for node in nodes }
    level : dict[int, int] = { node : 0 for node in nodes }
    ready : deque[int] = deque(node for node in nodes if waiting[node] == 0)
    result : dict[int, list[int]] = dict()
    while len(ready) != 0:
        node = ready.popleft()
        result.setdefault(level[node], []).append(node)
        step = 0 if node in barriers else 1
        for ref in sorted(refs[node]):
            level[ref] = max(level[ref], level[node] + step)
            waiting[ref] -= 1
            if waiting[ref] == 0: ready.append(ref)
    return [ result[index] for index in sorted(result) ]

def _order(tasks : list[Task]) -> list[list[int]]:
    """The positions of the tasks grouped into levels: each task after its
    dependencies among the tasks and after the tasks of the earlier stages
    of its flows. Raises RuntimeError if the dependencies form a cycle with
    the stage order, since no task of such a cycle can ever start."""
    uids : dict[Task, int] = { task : uid for uid, task in enumerate(tasks) }
    deps : dict[int, set[int]] = dict()
    refs : dict[int, set[int]] = dict()
    for uid, task in enumerate(tasks):
        deps[uid] = { uids[dep] for dep in task.get_deps() if dep in uids }
        refs[uid] = { uids[ref] for ref in task.get_refs() if ref in uids }

    # Within a flow, every task of a stage starts after the tasks of the
    # stages before it. One barrier node between consecutive stages orders
    # them, where an edge from every task of one stage to every task of the
    # next would make the graph quadratic in their size.
    stages_by_flow : dict[str, dict[int, list[int]]] = dict()
    for uid, task in enumerate(tasks):
        for flow, stage in task.get_flows().items():
            stages_by_flow.setdefault(flow, dict()).setdefault(stage, []).append(uid)
    next_uid = len(tasks)
    for flow in sorted(stages_by_flow.keys()):
        stages = stages_by_flow[flow]
        previous : list[int] | None = None
        for stage in sorted(stages.keys()):
            current = stages[stage]
            if previous is not None:
                barrier = next_uid
                next_uid += 1
                deps[barrier] = set(previous)
                refs[barrier] = set(current)
                for uid in previous: refs[uid].add(barrier)
                for uid in current: deps[uid].add(barrier)
            previous = current

    # Levels without the barriers
    barriers = set(range(len(tasks), next_uid))
    levels = _levels(sorted(deps.keys()), deps, refs, barriers)

    # Every dependency may respect the stages of the flows its two tasks
    # share, and still close a cycle with the stage order of other flows
    placed = { uid for level in levels for uid in level }
    stuck = [ uid for uid in range(len(tasks)) if uid not in placed ]
    if len(stuck) != 0:
        raise RuntimeError(
            'The dependencies of these tasks form a cycle with the stage '
            'order of their flows: %s' % ', '.join(sorted(
                '"%s"' % tasks[uid].description
                for uid in stuck if tasks[uid].description
            ))
        )
    return [
        [ uid for uid in level if uid < len(tasks) ]
        for level in levels
    ]

def check_stages(graph : Graph):
    """Raise RuntimeError if the dependencies of the tasks form a cycle with
    the stage order of their flows. compile only sees the tasks it is about
    to run, so this checks the whole graph, whatever is up to date."""
    _order(graph)

def compile(graph : Graph) -> Program:
    if has_cycle(graph):
        raise RuntimeError('Cycle detected in agenda!')

    # Every out-of-date task that can run. Propagation has already made the
    # tasks after an out-of-date task out of date, so no search is needed;
    # searching down from the tasks nothing depends on stopped at skipped
    # tasks and missed the tasks only they needed.
    tasks : list[Task] = [
        task for task in graph
        if not task.get_valid() and task.get_active()
    ]

    # Each batch holds the tasks whose dependencies all lie in earlier
    # batches; one sequence per task
    program : Program = []
    for level in _order(tasks):
        batch = [ [tasks[uid]] for uid in level ]
        if len(batch) != 0: program.append(batch)
    return program
