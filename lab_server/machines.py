from lib_operator import Operator


class HumanPlateServer(Operator):
    def __init__(self, id, manipulate_list):
        super().__init__(id, manipulate_list=manipulate_list, type="ServePlate96")


class TecanFluent480(Operator):
    def __init__(self, id, manipulate_list):
        super().__init__(id, manipulate_list=manipulate_list, type="DispenseLiquid96Wells")


class OpentronsOT2(Operator):
    def __init__(self, id, manipulate_list):
        super().__init__(id, manipulate_list=manipulate_list, type="DispenseLiquid96Wells")


class TecanInfinite200Pro(Operator):
    def __init__(self, id, manipulate_list):
        super().__init__(id, manipulate_list=manipulate_list, type="ReadAbsorbance3Colors")


class HumanStoreLabware(Operator):
    def __init__(self, id, manipulate_list):
        super().__init__(id, manipulate_list=manipulate_list, type="StoreLabware")


# machines = [
#     HumanPlateServer("human_plate_server", manipulate_list),
#     TecanFluent480("tecan_fluent_480", manipulate_list),
#     OpentronsOT2("opentrons_ot2", manipulate_list),
#     TecanInfinite200Pro("tecan_infinite_200_pro", manipulate_list),
#     HumanStoreLabware("human_store_labware", manipulate_list),
# ]
