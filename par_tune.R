source("tools.R")
method <- "par_tune_3";system(paste("mkdir ",method,sep=""));

#引入ACTIONS推荐，INSTALLS以供查重
	ads <- read.table('lib/ads',header=TRUE,row.names=1);no_of_ads <- as.integer(rownames(ads[nrow(ads),]));new_to_old_ads <- rev(as.integer(rownames(ads)));
	old.ads <- read.table('lib/old.ads',header=TRUE,row.names=1,na.strings=0);

	INSTALLS <- getIAasList("INSTALLS",ia_row,rip_first=TRUE);
	ACTIONS <- getIAasList("ACTIONS",ia_row,rip_first=TRUE);

	ACTIONS <- sapply(ACTIONS,function(x){return(as.integer(x)[as.integer(x) <= no_of_ads])});
	del_names <- c();
	for(i in 1:length(ACTIONS)){
		if(length(ACTIONS[[i]]) == 0){
			del_names <- append(del_names,names(ACTIONS[i]));
		}
	}
	INSTALLS[del_names] <- NULL;
	ACTIONS[del_names] <- NULL;
	print(paste("Total Have ",length(ACTIONS)," users",sep=""));
#引入ACTIONS推荐，INSTALLS以供查重


#A <- 用户安装列表的类别直接推荐
	INSTALL <- INSTALLS[A];
	recommend_by_category("INSTALL","A");
#A <- 用户安装列表的类别直接推荐

#B <- 用户广告行为的类别直接推荐
	ACTION <- ACTIONS[B];
	recommend_by_category("ACTION","B");
#B <- 用户广告行为的类别直接推荐

#C <- 单个广告行为的itemCF
	#sim <- generate_sim_matrix(ACTIONS,normalize=FALSE);
	sim <- read.table("par_tune_2/sim_matrix",header=TRUE,sep=",");dimnames(sim) <- list(c(1:nrow(sim)),c(1:ncol(sim)));sim<-as.matrix(sim);sim<-sim / apply(sim,1,max);sim[is.na(sim)] <- 0;
	recommend_for_users(ACTIONS[C],sim,meth="itemCF",INDICATOR=FALSE,output_file="C");
#C <- 单个广告行为的itemCF

#D <- NMF
	#INSTALL <- INSTALLS[D];ACTION <- ACTIONS[D];
	nmf <- read.table("par_tune_2/NMF_matrix",header=TRUE,sep=",");dimnames(nmf) <- list(c(1:nrow(nmf)),c(1:ncol(nmf)));nmf<-as.matrix(nmf);#nmf<-nmf / apply(nmf,1,max);nmf[is.na(nmf)] <- 0;
	recommend_for_users(INSTALLS[D],nmf,meth="NMF",INDICATOR=FALSE,output_file="D");
#D <- NMF

print(paste("重复用户的数量: ",assert_no_replicate(c("A","B","C","D")),sep=""));







