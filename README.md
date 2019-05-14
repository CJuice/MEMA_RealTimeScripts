# MEMA Real Time Processes

The Maryland Emergency Management Agency (MEMA) had processes built to capture data important
to emergency management from various entities and systems. Examples are USGS, NOAA, RITIS, 
and MIEMSS CHATS. The processes were aging and frequently breaking but their design did
not lend to easy revision. The scripts in thie project are redesigns of the original 
processes. Previously, all projects shared code modules so revisions for one process 
could affect the other processes in undesired ways. The new design breaks each process
into it's own self contained script that is independent of the code in other projects. 
All scripts are procedural and rely on a config file for sensitive information.