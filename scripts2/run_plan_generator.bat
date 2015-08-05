@echo off

set PROFILE_PATH=E:\impala_simulator\resource\profiles\text\profiles
set EXECUTION_PLAN_DIRETORY=%PROFILE_PATH%\execution_plans

IF NOT EXIST %EXECUTION_PLAN_DIRETORY% mkdir %EXECUTION_PLAN_DIRETORY%

FOR /R %PROFILE_PATH% %%f in (*.log) do (		
	del /Q %%~nf
	copy /Y %%f .
	python2 plan_generator.py %%~nf	
	echo %%f
	rem copy /Y %%f E:\impala_simulator\resource\profiles\text\files
)
copy /Y *.xml %EXECUTION_PLAN_DIRETORY%