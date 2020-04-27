#$ chunkify b=3 a b c d e f g h 
import qondor
print(qondor.get_preproc().chunks)
print(qondor.get_chunk())
