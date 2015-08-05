if (!exists("serverID")) serverID=''

set datafile separator ","
set terminal jpeg size 1280,960
set output 'output/Server'.serverID.'.jpg'
set multiplot layout 6,1 title 'Server'.serverID
set lmargin 8
set rmargin 10
set grid
set ytics border


set y2tics
plot 'output/disk'.serverID.'.csv' using 1:5 axis x1y1 with points lc rgb "red"  title "DiskId"
unset y2tics

set y2tics
plot 'output/scan'.serverID.'.csv' using 1:3 axis x1y1 with points lc rgb "red" title "NodeID"
unset y2tics


set y2tics
plot 'output/exchange'.serverID.'.csv' using 1:4 axis x1y1 with points lc rgb "green" lw 3 pt 2 title "EXH Node", \
							''  using 1:3 axis x1y2 with points lc rgb "red" lw 3 pt 2 title "Dest. Fragment"
							
unset y2tics							


unset yrange 
set y2tics
set style fill transparent solid 0.9 noborder
plot 'output/join'.serverID.'.csv' using 1:3 axis x1y1 with points lc rgb "red" title "HJ Node"						 
unset y2tics						 


set y2tics
plot 'output/aggregation'.serverID.'.csv' using 1:3 axis x1y1 with points lw 5 pt 4 lc rgb "blue" title "AGG Node"



set y2tics
plot 'output/sort'.serverID.'.csv' using 1:3 axis x1y1 with points lw 5 pt 4 lc rgb "blue" title "Sort Node"


unset multiplot 
reset
