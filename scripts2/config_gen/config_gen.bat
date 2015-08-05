@ECHO off
RMDIR configs
python config_gen.py config.template single_config.params
FOR /R configs %%G in (*.xml) do (	
	MOVE /Y "%%G" "..\..\ImpalaFrameworkDebug\config.xml"	
)
RMDIR configs
