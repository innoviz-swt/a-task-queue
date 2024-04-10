import starlette.datastructures


async def form_data_array(data: dict):
    ret = []
    for k, v in data.items():
        import pickle

        if isinstance(v, starlette.datastructures.UploadFile):
            v = await v.read()
        # expect format of index.key
        assert "." in k
        i, *k = k.split(".")
        i = int(i)
        k = ".".join(k)
        # expect monotonic rising items
        if i == len(ret):
            ret += [dict()]
        elif i == len(ret) - 1:
            pass
        else:
            # should never get here
            raise RuntimeError(f"unexpected item index '{i}'. len res: {len(ret)}")
        ret[i][k] = v

    return ret
