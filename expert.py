from experta import *

class SluggingExpertSystem(KnowledgeEngine):
    @DefFacts()
    def _initial_action(self):
        yield Fact(action="sluggingExpertSystem")

    # Define rules based on your conditions
    @Rule(Fact(action='sluggingExpertSystem'), Fact(fluctuation=0), Fact(choke_change=0))
    def rule_0(self):
        self.declare(Fact(result=0))

    @Rule(Fact(action='sluggingExpertSystem'), Fact(fluctuation=1), Fact(choke_change=1))
    def rule_1(self):
        self.declare(Fact(result=3))

    @Rule(Fact(action='sluggingExpertSystem'), Fact(fluctuation=0), Fact(choke_change=1))
    def rule_2(self):
        self.declare(Fact(result=0))

    @Rule(Fact(action='sluggingExpertSystem'), Fact(fluctuation=1), Fact(choke_change=0))
    def rule_3(self):
        self.declare(Fact(result=1))

    @Rule(Fact(action='sluggingExpertSystem'), Fact(result=MATCH.result))
    def show_result(self, result):
        self.result = result