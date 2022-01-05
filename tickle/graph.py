###############################################################################
# Functions
###############################################################################
def deps(task): return task._deps
def refs(task): return task._refs

def has_cycle(worklist):
    def _visit(task, path, visited):
        if task in visited: return False
        if task in path: return True
        _path = path.copy()
        _path.append(task)
        _visited = visited.copy()
        _visited.add(task)
        for dep in deps(task):
            if _visit(dep, _path, _visited): return True
        return False

    visited = set()
    for task in worklist:
        if _visit(task, list(), visited): return True
        visited.add(task)
    return False

def leafs(graph):
    return list(filter(lambda node: len(refs(node)) == 0, graph))

def propagate(graph):
    def __topological_order(worklist):
        result = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            if node in result: continue
            result.append(node)
            worklist += deps(node)
        return result

    # Defensively check for cycles
    worklist = leafs(graph)
    if has_cycle(worklist[:]):
        raise RuntimeError('Cycle detected in agenda!')

    # Propagate invalidity
    for dst in __topological_order(worklist):
        for src in deps(dst):
            if src.get_valid(): continue
            dst.set_valid(False)

def compile(graph):

    # Find tasks that needs to be performed
    def __reachable_alive(worklist):
        result = list()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in result: continue
            if task.get_valid(): continue
            result.append(task)
            worklist += deps(task)
        return list(reversed(result))

    # Find initial tasks
    def __find_roots(worklist, alive):
        result = set()
        visited = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            if task not in alive: continue
            visited.add(task)
            alive_deps = deps(task).intersection(alive)
            if len(alive_deps) == 0: result.add(task)
            else: worklist += alive_deps
        return result

    # Join sequential tasks into groups
    def __join_tasks(worklist, alive):
        result = list()
        groups = dict()
        visited = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            visited.add(task)
            worklist += refs(task).intersection(alive)
            alive_deps = list(deps(task).intersection(alive))
            if len(alive_deps) == 1:
                index = groups[alive_deps[0]]
                result[index].append(task)
                groups[task] = index
            else:
                index = len(result)
                result.append([task])
                groups[task] = index
        return result, groups

    # Sort task groups in topological order
    def __parallel_ordering(worklist, groups, group_map, alive):
        def __group_refs(group):
            last = groups[group][-1]
            alive_refs = refs(last).intersection(alive)
            return set( group_map[ref] for ref in alive_refs )
        def __group_deps(group):
            first = groups[group][0]
            alive_deps = deps(first).intersection(alive)
            return set( group_map[dep] for dep in alive_deps )
        result = dict()
        priorities = dict()
        visited = set()
        while len(worklist) != 0:
            group = worklist.pop(0)
            if group in visited: continue
            group_deps = __group_deps(group)
            if len(group_deps.difference(visited)) != 0: continue
            visited.add(group)
            worklist += __group_refs(group)
            priority = max([-1] + [ priorities[dep] for dep in group_deps ]) + 1
            if priority not in result: result[priority] = set()
            result[priority].add(group)
            priorities[group] = priority
        return [
            [ groups[group] for group in result[priority] ]
            for priority in range(len(result))
        ]

    targets = leafs(graph)
    if has_cycle(targets[:]):
        raise RuntimeError('Cycle detected in agenda!')
    alive = __reachable_alive(targets[:])
    roots = __find_roots(targets[:], alive)
    seqs, seq_map = __join_tasks(list(roots), alive)
    worklist = list(set( seq_map[root] for root in roots ))
    result = __parallel_ordering(worklist, seqs, seq_map, alive)
    return result
