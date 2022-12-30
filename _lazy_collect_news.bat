@echo Starting script..
@set StopWord="Stop_News.txt"
@set %RunScript="./src/modules/LoadData_fin.py"


@:loop
@echo News Collecting starts:

@echo DataLoader start:
@timeout 5

@python %RunScript%

@timeout 2000
@if exist %StopWord% ( goto ednLoop ) else ( @echo Continue.. )
@goto loop

@:ednLoop
@echo Stop File name found!!!  End of Script! Bye!
@echo zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz
@echo zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz
@echo zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz....

@pause
@exit