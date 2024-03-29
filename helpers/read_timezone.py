import platform

def read_timezone_from_file():
    try:
        if platform.system() == "Linux":
            with open("/etc/timezone", "r") as f:
                return f.read().rstrip()
        return "Etc/UTC"
    except:
        return "Etc/UTC"

