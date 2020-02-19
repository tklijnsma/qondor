#$ split_transactions this_is_item_1 this_is_item_2
#$ htcondor HOLD True
import qondor
preprocessing = qondor.preprocessing(__file__)
print preprocessing.get_item()