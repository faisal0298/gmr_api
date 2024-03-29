def convertToTime(seconds, without_seconds=False):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    if hour != 0:
        if without_seconds:
            return "%d hr %d min %d sec" % (hour, minutes, seconds)
        else:
            return "%d hr %d min" % (hour, minutes)
    elif minutes != 0:
        return "%d min %d sec" % (minutes, seconds)
    else:
        return "%d sec" % (seconds)


def convertToSeconds(time):
    for (index, values) in enumerate(time.split(":")):
        if index == 0:
            hours_to_seconds = int(values) * 3600
        elif index == 1:
            minutes_to_seconds = int(values) * 60
        else:
            seconds = int(values)

    return hours_to_seconds + minutes_to_seconds + seconds


def convertToHours(seconds):
    return "%02d" % (seconds / 3600)


def convertToMinutes(seconds):
    return "%02d" % (seconds / 60)
