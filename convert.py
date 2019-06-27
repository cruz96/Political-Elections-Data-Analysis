def convertInt(date):
    timestamp = str(date)
    year = timestamp[-4:]
    timestamp = round(int(date)/10000)
    timestamp = str(timestamp)
    day = timestamp[-2:]
    timestamp = round(int(timestamp)/100)
    month = int(timestamp)
    newDate = year + "-" + "{:0>2d}".format(month) + "-" + day
    return(newDate)
    
def convertTimestamp(date):
    timestamp = str(date[0:11])
    return(timestamp)

def main():
    print(convertInt(3162018))
    print(convertInt(3092018))
    print(convertInt(11302018))
    print(convertTimestamp("2012-06-10 00:00:00 UTC"))
main()