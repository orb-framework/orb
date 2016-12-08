import demandimport

with demandimport.enabled():
    import orb
    import pytz


def localize_timezone(dtime, tz=None):
    """
    Converts the given dtime to the timezone provided.

    Args:
        dtime: <datetime.datetime>
        tz: <pytz.timezone> or None

    Returns:
        <datetime.datetime>

    """
    if tz is None:
        return dtime
    elif dtime.tzinfo is not None:
        return dtime.astimezone(tz)
    else:
        server_tz = pytz.timezone(orb.system.settings.server_timezone)
        local_time = server_tz.localize(dtime)
        return local_time if server_tz == tz else local_time.astimezone(tz)


def utc_timezone(dtime):
    """
    Converts the given datetime to UTC, stripping the timezone information
    from the converted datetime.

    Args:
        dtime: <datetime.datetime>

    Returns:
        <datetime.datetime>

    """
    # if no timezone information is provided, assume the time is based on the server time
    if dtime.tzinfo is None:
        tz = pytz.timezone(orb.system.settings.server_timezone)
        dtime = tz.localize(dtime)

    return dtime.astimezone(pytz.utc).replace(tzinfo=None)