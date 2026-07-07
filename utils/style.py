import colored


class PrettyText:
    @classmethod
    def normal(cls, data, pretext: str = '+'):
        print(
            colored.Fore.LIGHT_BLUE + colored.Style.BOLD + '[' + colored.Fore.purple_1a + pretext + colored.Fore.LIGHT_BLUE + '] ' + colored.Style.RESET + str(
                data))
    @classmethod

    def error(cls, data, pretext: str = '!'):
        print(
            colored.Fore.RED_1 + colored.Style.BOLD + '[' + colored.Fore.WHITE + pretext + colored.Fore.RED_1 + '] ' + colored.Style.RESET + str(
                data))

    @classmethod
    def good(cls, data, pretext: str = '~'):
        print(
            colored.Fore.spring_green_3b + colored.Style.BOLD + '[' + colored.Fore.MAGENTA + pretext + colored.Fore.spring_green_3b + '] ' + colored.Style.RESET + str(
                data))

    @classmethod
    def success(cls, data, pretext: str = '~'):
        print(
            colored.Fore.yellow_1 + colored.Style.BOLD + '[' + colored.Fore.cyan_3 + pretext + colored.Fore.yellow_1 + '] ' + colored.Style.RESET + str(
                data))

    @classmethod
    def warning(cls, data, pretext: str = '*'):
        print(
            colored.Fore.VIOLET + colored.Style.BOLD + '[' + colored.Fore.YELLOW + pretext + colored.Fore.VIOLET + '] ' + colored.Style.RESET + str(
                data))

    @classmethod
    def print(cls, data, color):
        print(getattr(colored.Fore, color) + str(data) + colored.Style.RESET)

