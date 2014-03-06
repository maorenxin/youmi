
source("tools.R")
local <- FALSE;
if(local){
	ROW <- -1;
	a_cut <- 10000;
	i_cut <- 5000;
	loc = paste("/mnt/spring/",sep="");
}else{
	ROW <- -1;
	a_cut <- 10000;
	i_cut <- 5000;
	loc = paste("/mnt/spring/",sep="");
}

#构建ITEM相似度矩阵


transform_log_to_a(data="action_data",result="ACTIONS",formmatrix=FALSE,cut_to="351865051130574");
transform_log_to_i(data="install_data",result="INSTALLS",formmatrix=FALSE);




