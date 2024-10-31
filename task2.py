import re

if __name__ == "__main__":    
    pattern = re.compile(r'Failed password for root from (\d+\.\d+\.\d+\.\d+)')
    
    attempts = {}
    with open("openssh.log", 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                ip = match.group(1)
                if ip in attempts:
                    attempts[ip] += 1
                else:
                    attempts[ip] = 1
    
    print(attempts)
