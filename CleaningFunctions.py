def senateNameClean(input):
    input = input.upper()
    split = input.split(' ')
    
    #removes all %.
    badElements = []
    for i in range(len(split)):
        element = split[i]
        if("." in element):
            badElements.append(i)
        #remove II III and IV
        elif(element == "II" or element == "III" or element == "IV" or element == "JR" or element == "SR"):
            badElements.append(i)
    #remove bad elements
    for i in reversed(badElements):
        split.pop(i)
        
    for i in range(len(split)):
        if('"' in split[i]):
            temp = split[0]
            split[0] = split[i].replace('"', '')
            split[i] = temp
    
    #removes " element if more than 3 elements
    badElements = []
    if(len(split) == 2):
        #removes "
        for i in range(len(split)):
            split[i] = split[i].replace('"', '')
            split[i] = split[i].replace('(', '')
            split[i] = split[i].replace(')', '')
    elif(len(split) == 1):
        split = [""]
    else:
        for i in range(len(split)):
            element = split[i]
            if(')' in element or '(' in element): #'"' in element or 
                badElements.append(i)
        for i in reversed(badElements):
            split.pop(i)
    if(len(split) == 3):
        split.pop(1)
    
    #remove all other characters
    for i in range(len(split)):
        split[i] = split[i].replace(',', '')
        split[i] = split[i].replace("'", '')
        #remove name after -
        if("-" in split[i]):
            split[i] = split[i].split("-")[0]
            
    #convert list to string
    output = ""
    for i in range(len(split)):
        if(i != len(split)-1):
            output += split[i] + " "
        else:
            output += split[i]
            
    return output
    
def candidateSummaryCleanName(input):
    firstsplit = input.split('/')
    firstsplit = firstsplit[0]
    split = firstsplit.split(' ')
    
        #removes all %.
    badElements = []
    for i in range(len(split)):
        element = split[i]
        if("." in element):
            badElements.append(i)
        #remove II III and IV
        elif(element == "II" or element == "III" or element == "IV" or element == "JR" or element == "SR"):
            badElements.append(i)
        #swap first name with preferred name
        elif('"' in element):
            temp = split[1]
            split[1] = element
            split[i] = temp
    #remove bad elements
    for i in reversed(badElements):
        split.pop(i)
        
    #remove unecessary characters
    for i in range(len(split)):
        split[i] = split[i].replace(',', '')
        split[i] = split[i].replace("'", '')
        split[i] = split[i].replace('"', '')
        #remove name after -
        if("-" in split[i]):
            split[i] = split[i].split("-")[0]
        
    #remove elements from end to beginning
    while(len(split)> 2):
        split.pop(-1)
        
    #convert list to string
    output = ""
    for i in reversed(range(len(split))):
        if(i != 0):
            output += split[i] + " "
        else:
            output += split[i]

    return output
        
def main():
    input = 'E. J. "Jake" Garn'
    print(senateNameClean(input))
    input = 'James W. "Jim" Wright'
    print(senateNameClean(input))
    input = 'L. D. "Nota" Knox, Sr.'
    print(senateNameClean(input))
    input = 'Anna E. Throne-Holst'
    print(senateNameClean(input))
    input = 'Blank Vote/Scattering'
    print(senateNameClean(input))
    input = '"Joseph" Cao'
    print(senateNameClean(input))
    input = 'Robert E. (Bud) Cramer, Jr'
    print(senateNameClean(input))
    input = 'Catherine "CAT" Ping'
    print(senateNameClean(input))
    input = 'Pete Glidewell'
    print(senateNameClean(input))
    print
    
    input = 'CRAMER, ROBERT E "BUD" JR'
    print(candidateSummaryCleanName(input))
    input = 'PING, CATHERINE LOUISE "CAT"'
    print(candidateSummaryCleanName(input))
    input = 'GLIDEWELL, POWELL WATKINS "PETE" III'
    print(candidateSummaryCleanName(input))
    input = 'CLIFTON, TIRELL ALEXANDER MAXWELL REV. DR. I'
    print(candidateSummaryCleanName(input))
main()