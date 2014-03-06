source("tools.R")

#参数申明


ACTIONS <- scan(paste(loc,"lib/ACTIONS_raw",sep=""),what=character(0),nlines=-1,sep="");
ACTIONS <- as.list(ACTIONS);
ACTIONS <- strsplit(sapply(ACTIONS,"[",1),",");
ACTIONS_user <- sapply(ACTIONS,"[",1);
names(ACTIONS) <- ACTIONS_user;

INSTALLS <- scan(paste(loc,"lib/INSTALLS_raw",sep=""),what=character(0),nlines=-1,sep="");
INSTALLS <- as.list(INSTALLS);
INSTALLS <- strsplit(sapply(INSTALLS,"[",1),",");
INSTALLS_user <- sapply(INSTALLS,"[",1);
names(INSTALLS) <- INSTALLS_user;

#寻找交集
IA_NAME <- intersect(ACTIONS_user,INSTALLS_user);
IA_NAME <- sample(IA_NAME);
#寻找交集

ACTIONS <- ACTIONS[IA_NAME]
INSTALLS <- INSTALLS[IA_NAME]

count <- 1;
for(a in ACTIONS){
	if(count == 1){
		write.table(t(as.matrix(a)),paste(loc,"lib/ACTIONS",sep=""),sep=",",append=FALSE,col.names=FALSE,row.names=FALSE,quote=FALSE)
	}else{
		write.table(t(as.matrix(a)),paste(loc,"lib/ACTIONS",sep=""),sep=",",append=TRUE,col.names=FALSE,row.names=FALSE,quote=FALSE)
	}
	count <- count + 1;
}

count <- 1;
for(i in INSTALLS){
	if(count == 1){
		write.table(t(as.matrix(i)),paste(loc,"lib/INSTALLS",sep=""),sep=",",append=FALSE,col.names=FALSE,row.names=FALSE,quote=FALSE)
	}else{
		write.table(t(as.matrix(i)),paste(loc,"lib/INSTALLS",sep=""),sep=",",append=TRUE,col.names=FALSE,row.names=FALSE,quote=FALSE)
	}
	count <- count + 1;
}
