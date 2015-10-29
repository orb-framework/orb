import orb
import projex.rest


# assign record encode/decoders
def record_encoder(py_obj):
    # encode a record
    if orb.Table.recordcheck(py_obj) or orb.View.recordcheck(py_obj):
        return True, py_obj.json()
    # encode a recordset
    elif orb.RecordSet.typecheck(py_obj):
        return True, py_obj.json()
    # encode a query
    elif orb.Query.typecheck(py_obj):
        return True, py_obj.toDict()
    return False, None

projex.rest.register(record_encoder)
