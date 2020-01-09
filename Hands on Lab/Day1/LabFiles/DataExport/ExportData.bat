mysql --local-infile=1 -udemouser -pDemo@pass123 -h localhost  < ExportData.sql
mkdir C:\Migration
xcopy "C:\ProgramData\MySQL\MySQL Server 8.0\Uploads" "C:\Migration" /s /h /e /y