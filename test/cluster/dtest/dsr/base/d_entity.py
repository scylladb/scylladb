def _deserialize(value, class_mapping: dict, hash_map=None):  # noqa: PLR0911, PLR0912
    """
    Build instance from state
    """
    if hash_map is None:
        hash_map = {}
    if isinstance(value, dict):
        cl_name = value.get("__class__", None)
        if cl_name is not None:
            cl = class_mapping.get(cl_name, None)
            if cl is None:
                raise ValueError(f"Unknown class {cl_name}")
            return cl
        cl_name = value.get("__instance__", None)
        if cl_name:
            cl = class_mapping.get(cl_name, None)
            if cl is None:
                raise ValueError(f"Unknown class {cl_name}")
            instance = cl()
            hash_num = value.get("__hash__")
            if hash_num is not None:
                hash_map[hash_num] = instance
            output = {}
            for attr_name, attr_value in value.items():
                if attr_name[0] != "_":
                    output[attr_name] = _deserialize(attr_value, class_mapping, hash_map)
            instance._set_attributes(output)
            return instance
        output = {}
        for attr_name, attr_value in value.items():
            attr_name = _deserialize(attr_name, class_mapping, hash_map)  # noqa: PLW2901
            output[attr_name] = _deserialize(attr_value, class_mapping, hash_map)
        return output

    elif isinstance(value, str):
        if not value.startswith("__reference__<"):
            return value
        hash_num = int(value[14:-1])
        if hash_num is not None:
            instance = hash_map.get(hash_num, None)
            if instance is None:
                raise ValueError(f"Unknown hash {hash_num}")
            return instance
    elif isinstance(value, list):
        return [_deserialize(el, class_mapping, hash_map) for el in value]
    elif isinstance(value, tuple):
        return tuple([_deserialize(el, class_mapping, hash_map) for el in value])
    return value


def _serialize(instance, hash_map=None):  # noqa: PLR0911
    """
    Build state from instance
    """
    if isinstance(instance, type):
        return {"__class__": instance.__name__}
    elif isinstance(instance, DEntity):
        if hash_map is None:
            return None
        instance_hash = hash_map.get(instance, None)
        if instance_hash is None:
            hash_map[instance] = hash(instance)
            output = {"__instance__": instance.__class__.__name__, "__hash__": hash_map[instance]}
            for attr_name, attr_value in instance.__dict__.items():
                if attr_name[0] == "_" or not hasattr(instance.__class__, attr_name):
                    continue
                output[attr_name] = _serialize(attr_value, hash_map)
            return output
        return f"__reference__<{instance_hash}>"
    elif isinstance(instance, list):
        return [_serialize(el, hash_map) for el in instance]
    elif isinstance(instance, tuple):
        return tuple([_serialize(el, hash_map) for el in instance])
    elif isinstance(instance, dict):
        return {_serialize(key, hash_map): _serialize(value, hash_map) for key, value in instance.items()}
    return instance


class DEntity:
    """
    Deterministic Entity
    Protocol that allows random entity being deterministic.
    Determination is achieved via serialization and deserialization, as desirable affect of that side affect
     such entities could be saved into python simple data structure and restored from it.
    """

    _get_random = True
    _registered_classes = {}

    def __init__(self, **kwargs):
        self._set_attributes(kwargs)

    def __init_subclass__(cls, **kwargs):
        cls._registered_classes[cls.__name__] = cls

    def __hash__(self):
        output = [("__instance__", self.__class__.__name__)]
        for attr_name, attr_value in self.__dict__.items():
            if attr_name[0] == "_" or not hasattr(self.__class__, attr_name):
                continue
            output.append((attr_name, _serialize(attr_value)))
        output = sorted(output, key=lambda x: x[0])
        return hash(repr(output))

    @staticmethod
    def _get_hash_of_instance(instance):
        """
        Calculate instance hash. Needed to handle references to objects
        """
        # TBD: Create single framework for _instance_to_value and _get_hash_of_instance
        output = [("__instance__", instance.__class__.__name__)]
        for attr_name, attr_value in instance.__dict__.items():
            if attr_name[0] == "_" or not hasattr(instance.__class__, attr_name):
                continue
            output.append((attr_name, _serialize(attr_value)))
        output = sorted(output, key=lambda x: x[0])
        return hash(repr(output))

    def _set_attributes(self, kwargs):
        for attr_name, attr_value in kwargs.items():
            if attr_name[0] == "_" or not hasattr(self.__class__, attr_name):
                continue
            setattr(self, attr_name, attr_value)

    @classmethod
    def load(cls, state):
        """
        Recover instances from saved state
        """
        return _deserialize(state, cls._registered_classes, {})

    def save(self):
        """
        Save instance state, so that it could be recovered
        """
        return _serialize(self, {})

    def copy(self):
        """
        Makes copy of the instance
        """
        return self.load(self.save())

    def check_validity(self):
        """
        Run validation of the instance and raises exception if instance is not valid
        """
