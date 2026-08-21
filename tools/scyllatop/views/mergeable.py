class Mergeable(dict):
    def __init__(self, mergeMethod):
        self._mergeMethod = mergeMethod
        dict.__init__(self)

    def add(self, dictionary):
        for key, value in dictionary.items():
            self.setdefault(key, [])
            self[key].append(value)

    def merged(self):
        result = {}
        for key, values in self.items():
            result[key] = self._mergeMethod(values)
        return result
