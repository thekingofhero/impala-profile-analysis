def get_time_by_unit(number, unit):
    if unit == 'us':
        return number * 1000
    elif unit == 'ms':
        return number * 1000000
    elif unit == 'ns':
        return number
    elif unit == 'm':
        return number * 1000000000 * 60
    else: #second
        return number * 1000000000

def get_time(stripped_line):    
    
    tmp_idx = 0
    tmp = stripped_line.strip()
    time = 0
    number = ''
    
    for index in range(len(tmp)):
        
        if (tmp[index] == 's' and not tmp[index-1].isdigit()):
            continue

        if tmp[index].isdigit() or tmp[index] == '.':
            number += tmp[index]
            if number.startswith('0') and len(number) > 1:
                number = number[1:]
        elif tmp[index] == 'u' and tmp[index+1] == 's':
            time += get_time_by_unit(float(number), 'us')
            number = ''
        elif tmp[index] == 'n' and tmp[index+1] == 's':
            time += get_time_by_unit(float(number), 'ns')
            number = ''
        elif tmp[index] == 'm' and (index+1 == len(tmp) or tmp[index+1] != 's'):
             time += get_time_by_unit(float(number), 'm')
             number = ''
        elif tmp[index] == 'm' and tmp[index+1] == 's':
            time += get_time_by_unit(float(number), 'ms')
            number = ''        
        elif tmp[index] == 's' and (index+1 == len(tmp) or tmp[index+1] != 's'):
            time += get_time_by_unit(float(number), 's')        
            number = ''    
    return str(time)

def get_time_by_second(stripped_line):
    return float(get_time(stripped_line))/1000000000
