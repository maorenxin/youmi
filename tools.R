library(RMySQL);
#library(flexclust);
options(scipen=200);

local <- FALSE;
if(local){
	ROW <- 100;
	NMF_ROW <- 2;
	loc <- "/mnt/spring/";

	a_cut <- 20000;
	i_cut <- 5000;

	ia_row <- 1000;

	A <- c(100:200);
	B <- c(201:400);
	C <- c(401:600);
	D <- c(601:800);
	E <- c(801:1000);
}else{
	ROW <- -1;
	NMF_ROW <- 100000;
	loc <- "/mnt/spring/";

	a_cut <- 20000;
	i_cut <- 5000;

	ia_row <- -1;

	A <- c(1:110000);
	B <- c(110001:220000);
	C <- c(220001:330000);
	D <- c(330001:440000);
	E <- c(440001:550000);
}

GROUPS <- 3#抽样三组数据
SAMPLE <- 0.05#每组在50万用户中抽样5%

connectDb <- function(db="youmi"){
	return(dbConnect(MySQL(), user='root', password='', dbname=db, host="127.0.0.1"));
}

getFromDb <- function(query,db="youmi",stripfirst = FALSE){
	con <- connectDb(db);
	target <- dbGetQuery(con,query);
	dbDisconnect(con);
	if(stripfirst){target <- target[-1];}
	return(target);
}

category <- getFromDb("SELECT * FROM category;", stripfirst = TRUE);#22个分类对应的序号

ads <- getFromDb("SELECT * FROM ads;", stripfirst = TRUE);no_of_ads <- as.integer(rownames(ads[nrow(ads),]));#所有242广告对应的分类和收益

packages <- getFromDb("SELECT package, youmi_category AS category FROM packages;");#所有1283个用户安装APP对应的分类

factor <- ads[,"action"] / ads[,"display"];factor[is.na(factor) | factor ==0 | factor == Inf] <- 1;#根据转化率调整用户的喜好

writeToDb <- function(data,table,rownamesAsKey=FALSE){
	con <- connectDb();
	dbWriteTable(con, table, data, append=FALSE, overwrite=TRUE);
	if(rownamesAsKey){
		getFromDb(paste("ALTER TABLE `",table,"` CHANGE `row_names` `row_names` VARCHAR(128) NOT NULL;",sep=""));
		getFromDb(paste("CREATE INDEX row_names ON ",table,"(row_names);",sep=""));
	}
	dbDisconnect(con);
}

readTable <- function(table){
	con <- connectDb();
	target <- dbReadTable(con, table);
	dbDisconnect(con);
	return(target);
}

cut_duplicate_install_list <- function(source,id,adjust,method="combine"){
	del_list <- c();
	for(i in 3:nrow(source)){
		if((source[i,id] == source[i-1,id])&(source[i,id] != source[i-2,id])){
			duplicate <- source[source[,id] == source[i,id],adjust,drop=FALSE];
			del <- rownames(duplicate);
			comb_value <- combine_install_list(as.vector(as.matrix(duplicate)), sep=",");
			levels(source[,adjust]) <- c(levels(source[,adjust]),comb_value);
			source[i-1,adjust] <- comb_value;
			del_list <- append(del_list,del[-1]);
		}
		if(i %% 1000 == 0){print(paste("重复用户安装列表合并，progress ",100*i/nrow(source),"%",sep=""));}#输出进度
	}
	target <- source[-as.integer(del_list),];
	rownames(target) <- c(1:nrow(target));
	return(target);
}
combine_install_list <- function(source,sep=","){
	for(i in 1:(length(source)-1)){
		m1 <- strsplit(source[i],sep)[[1]];
		m2 <- strsplit(source[i+1],sep)[[1]];
		m_union <- union(m1, m2);
		source[i+1] <- paste(m_union,collapse=",");
	}
	return(source[length(source)]);
}

formation_of_INSTALLS <- function(source,id,category,packages){
	target <- matrix(0,nr=0,nc=nrow(category));
	for(i in 1:nrow(source)){
		tmp <- matrix(0,nr=1,nc=nrow(category),dimnames=list(rownames(source)[i],c(1:nrow(category))));
		apps <- strsplit(source[i,id],",")[[1]];
		if(!is.na(apps)){
			for(app in apps){
					cat <- packages[packages["package"] == app,"category"];
					tmp[,cat] <- tmp[,cat] + 1;
				}
		}
		target <- rbind(target,tmp);
		if(i %% 1000 == 0){print(paste("形成INSTALLS，progress ",100*i/nrow(source),"%",sep=""));}#输出进度
	}
	target <- as.data.frame(target);
	return(target);
}

convertToCatgory <- function(source, category, tags, factorAdjust = FALSE){
	target <- matrix(,nc=0,nr=nrow(source));
	for(i in 1:nrow(category)){
	  cat_cols <- as.matrix(apply(source[,tags["category"] == i,drop=FALSE],1,sum));
	  colnames(cat_cols) <- i;
	  if(factorAdjust){
	  	display_action <- apply(ads[ads["category"] == i,c("display","action")],2,sum);
	  	factor <- display_action[2] / display_action[1];
		if(!is.na(factor)){cat_cols <- cat_cols / factor;}
	  }
	  target <- cbind(target,cat_cols);
	}

	return(target);
}

randompick <- function(source,rate){
	sample_row <- sample(1:nrow(source),as.integer(nrow(USER_PREF) * rate));
	return(source[sample_row,,drop=FALSE]);
}

recommend <- function(user,cl,top_cat=3,select=5,method){
	#5个slot按权重抽前3
	weight <- centers[cl,order(centers[cl,],decreasing = T),drop=FALSE];
	top3 <- weight[,1:top_cat,drop=FALSE];
	top3 <- top3 / apply(top3,1,sum);
	slot <- sample_recommend_5_category(top3,select);
	#取出推荐列表，带权重
	for(s in 1:select){
		recom_every_s <- unique(ads[ads["category"] == slot[s],c("package","profit")]);
		if(method == "weight_select"){
			recom_every_s[2] <- recom_every_s[2] / apply(recom_every_s[2],2,sum);
			slot[,s] <- as.integer(sample(rownames(recom_every_s),1,prob = recom_every_s[,"profit"]));
			while(length(which(slot[1:s] == slot[,s]))>1){
				slot[,s] <- as.integer(sample(rownames(recom_every_s),1,prob = recom_every_s[,"profit"]));
			}
		}else if(method == "top_select"){
			recom_every_s <- recom_every_s[order(recom_every_s["profit"],decreasing = TRUE),,drop=FALSE];
			slot[,s] <- rownames(recom_every_s)[1]
			while(length(which(slot[1:s] == slot[,s]))>1){
				recom_every_s <- recom_every_s[-1,]
				slot[,s] <- as.integer(rownames(recom_every_s)[1]);
			}
		}
	}	
	return(as.vector(slot));
}

sample_recommend_5_category <- function(top3,select){
	slot <- matrix(NA,nc=select,nr=1,dimnames=list(c("recommend_5"),c(1:5)));
	for (s in 1:select){
		pick <- sample(colnames(top3),1,prob=as.vector(top3));
		slot[s] <- as.integer(pick);
	}
	return(slot);
}

user_action <- function(user){
	user_action <- getFromDb(paste("SELECT * FROM youmi WHERE youmi.uid = '",user,"';",sep=""));
	if(nrow(user_action) == 0){
		return(c())
	}else if(nrow(user_action) > 1){
		for(i in 2:nrow(user_action)){
			user_action[i,-1] <- user_action[i,-1,drop=FALSE] | user_action[i-1,-1,drop=F];
		}
		user_action <- user_action[nrow(user_action),,drop=FALSE];
	}
	rownames(user_action) <- as.list(user_action["uid"])[[1]];user_action <- user_action[-1];
	user_action <- which(user_action == 1)
	return(user_action)
}


calculatePRF_none <- function(user,cl,method="weight_select"){
	slot <- recommend(user,cl,top_cat=3,select=5,method);
	action <- user_action(user);
	A <- intersect(slot,action);
	B <- setdiff(slot,action);
	C <- setdiff(action,slot);
	prec <- length(A) / (length(A) + length(B));
	reca <- length(A) / (length(A) + length(C));
	f_sc <- 2 * prec * reca / (prec + reca);
	if(is.na(f_sc)){f_sc <- 0;}
	prf <- matrix(c(prec,reca,f_sc),nr=1,nc=3,dimnames=list(c(),c("P","R","F")));
	return(prf);
}

calculatePRF <- function(slot,action){
	
	A <- intersect(slot,action);
	B <- setdiff(slot,action);
	C <- setdiff(action,slot);
	prec <- length(A) / (length(A) + length(B));
	reca <- length(A) / (length(A) + length(C));
	f_sc <- 2 * prec * reca / (prec + reca);
	if(is.na(reca)){reca <- 0;};if(is.na(f_sc)){f_sc <- 0;};
	prf <- matrix(c(prec,reca,f_sc),nr=1,nc=3,dimnames=list(c(),c("P","R","F")));
	return(prf);
}

combine <- function(a,b,ratio,scale=FALSE){
	if(scale){
		a <- a / apply(a, 1, sum);
		a[is.na(a)]<-0;
		b <- b / apply(b, 1, sum);
		b[is.na(b)]<-0;
	}
	target <- a * ratio + b * (1 - ratio);
	colnames(target) <- c(1:nrow(category));
	return(target);
}

generateSplit <- function(USER_PREF,rec_cats){
	split <- matrix(,nr=0,nc=4,dimnames=list(c(),c("用户最大类","人数","Cluster","Cluster特征类")));
	sample_row <- sample(1:nrow(USER_PREF),as.integer(nrow(USER_PREF)/100));#1000000万行数据运行时间太长，直接取10000行
	tmp_top <- rec_cats[,1];
	count <- 0;
	for(r in sample_row){
		count = count + 1;
		if(count %% 100 == 0){print(paste("分裂矩阵，progress ",100*count/length(sample_row),"%, under k = ",k_value," and ratio = (",ratio," , ",1-ratio,") ",sep=""));}#输出进度
    	names <- as.integer(colnames(USER_PREF[r,which(USER_PREF[r,1:nrow(category)] == max(USER_PREF[r,1:nrow(category)])),drop=FALSE]));#对每一行取最大值的类别名
		kk <- cluster[r];#对每一行取分类数值
		kk_pos <- as.integer(tmp_top[kk]);#找到kk的特征类别
		#生成一个分裂矩阵
		for(i in 1:length(names)){
			split <- rbind(split,matrix(c(names[i], as.numeric(1 / length(names)), kk, kk_pos),nr=1,nc=4))
		}
		#生成一个分裂矩阵
	}
	return(split);
}

PRF <- function(split){
	prec_reca <- matrix(0,nc=10,nr=k_value,dimnames=list(paste("Cluster", c(1:k_value)), c("TOP1", "A", "B", "C", "D", "precision", "recall","av_prec","av_reca","f_score")));
    prec_reca[,"TOP1"] <- rec_cats[,1];
    tmp_top <- rec_cats[,1];
	for(k in 1:k_value){
		others <- which(tmp_top == tmp_top[k])[which(which(tmp_top == tmp_top[k]) != k)] #找到k时与他相同特征值的其他Cluster
		if(length(others) >0){pr <- split[split[,3] != others,,drop=FALSE];}else{pr <- split;}#去掉与当前k重复特征值的其他Cluster
		#求Prec & Recall
		prec_reca[k,"precision"] <- sum(pr[pr[,3] == k & pr[,1] == pr[,4],,drop=FALSE][,2]) / sum(pr[pr[,3] == k,,drop=FALSE][,2]);
		prec_reca[k,"recall"] <- sum(pr[pr[,3] == k & pr[,1] == pr[,4],,drop=FALSE][,2]) / sum(pr[pr[,1] == pr[pr[,3] == k,4][1],,drop=FALSE][,2]);
		#求Prec & Recall
	}
	prec_reca[,"av_prec"] <- average_precision <- weighted.mean(as.numeric(prec_reca[,"precision"]),clusize);#准确率
	prec_reca[,"av_reca"] <- average_recall <- weighted.mean(as.numeric(prec_reca[,"recall"]),clusize);#召回率
	prec_reca[,"f_score"] <- f_score <- 2 * (average_precision * average_recall) / (average_precision + average_recall);
	print("准确率和召回率计算完毕")
	return(prec_reca);
}

roundtoone <- function(m){
	sum <- apply(m,2,sum);
	for(r in 1:nrow(m)){
		m[r,] <- m[r,] / sum;
	}
	m[is.nan(m)] <-0
	return(m);
}


recommend_categories_per_cluster <- function(centers,name=FALSE){
	rec_cats <- matrix(,nc = nrow(category), nr = nrow(centers),dimnames=list(c(1:nrow(centers)),paste("TOP",c(1:nrow(category)),sep="")));
	rec_cats_name <- matrix(,nc = nrow(category), nr = nrow(centers),dimnames=list(c(1:nrow(centers)),paste("TOP",c(1:nrow(category)),sep="")));
	for (k in 1:nrow(centers)){
		rec_cats[k,] <- as.integer(names(centers[k,order(centers[k,],decreasing=TRUE)]));
		for(c in 1:nrow(category)){
			rec_cats_name[k,c] <- category[rec_cats[k,c],];
		}
	}
	if(name){
		return(as.data.frame(rec_cats_name))
	}else{
		return(as.data.frame(rec_cats))
	}
}

categorytoads <- function(source,name){
	pacs <- c();
	for(c in 1:length(source)){
		pac <- unique(ads[ads["category"] == source[c],c("package","profit")]);
		pac <- pac[order(pac["profit"],decreasing=TRUE),];
		if(length(pac) == 0){continue;}
		if(name){
			pac <- pac[,"package"];
		}else{
			pac <- as.integer(rownames(pac));
		}	
		pacs <- append(pacs,pac);
	}
	return(pacs)
}

recommend_packages_per_cluster <- function(rec_cats,name=FALSE){
	target <- list();
	for(k in 1:nrow(rec_cats)){
		target[[k]] <- categorytoads(as.vector(as.matrix(rec_cats[k,])),name=name);
	}
	return(target)
}

connectwithcomma <- function(a){
	for(i in 1:length(a)){if(i==1){target <- a[i]}else{target<-paste(target,a[i],sep=",")}}
	return(target)
}

listtodataframe <- function(source){
	a <- matrix(,nr=length(source),nc=length(source[[1]]),dimnames=list(c(1:length(source)),c(1:length(source[[1]]))));
	for (i in 1:length(source)){
		for(j in 1:length(source[[i]])){
			a[i,j] <- source[[i]][j]
		}
	}
	return(a)
}

printPRF <- function(db,col){
	prfs <- getFromDb(paste("SELECT `",col,"` FROM ",db," ",sep=""));
	prfs <- prfs[,1];prfs<-strsplit(prfs,split=",")
	p <- mean(as.numeric(sapply(prfs,"[",1)));
	r <- mean(as.numeric(sapply(prfs,"[",2)));
	f <- mean(as.numeric(sapply(prfs,"[",3)));
	print(paste("准确率=",100 * p,"% | 召回率=",100 * r,"% | F=",100 * f,"%",sep=""));
}

idtopackage <- function(rec_list,ads){
	target <- c();
	for(id in rec_list){
		target <- append(target,ads[id,"package"])
	}
	return(target);
}

packagetoid <- function(list_name,index){
	target <- c();
	for(pac in list_name){
		target <- append(target,rownames(index[index[,"package"] == pac,]))
	}
	target <- as.integer(target);
	return(target);
}


generate_sim_matrix <- function(sour,normalize=FALSE){
	target <- matrix(0,nc=no_of_ads,nr=no_of_ads,dimnames = list(c(1:no_of_ads),c(1:no_of_ads)));
	count <- 0;
	ptm<-proc.time();
	for (a in sour){
		pairs <- as.integer(a);pairs<-unique(pairs);#pairs <- pairs[pairs <= no_of_ads];
		if(length(pairs) > 1){
			for (i in 1:(length(pairs)-1)){
				for (j in (i+1):length(pairs)){		
					target[pairs[i],pairs[j]] <- target[pairs[i],pairs[j]] + 1;
					target[pairs[j],pairs[i]] <- target[pairs[j],pairs[i]] + 1;
				}
			}
		}
	count <- count + 1;
	if(count %% 100000 == 0){print(paste("生成相似度矩阵完成",round(100*count/length(sour),2),"%",sep=""));}#输出进度
	}
	print(paste("生成",method,"矩阵完成100%,用时",round((proc.time() - ptm)[3],2),"s",sep=""));


	if(normalize){
		target <- target / apply(target,1,max);target[is.na(target)] <- 0;
	}
	
	write.table(target,paste(loc,method,"/sim_matrix",sep=""),append=FALSE,row.names=TRUE,col.names=TRUE,sep=",");

	return(target);
}

generate_character_matrix <- function(INSTALLS,ACTIONS){
	target <- matrix(0,nr=nrow(packages),nc=nrow(ads),dimnames=list(c(1:nrow(packages)),c(1:nrow(ads))));
	for(r in 1:length(INSTALLS)){
		a <- as.integer(INSTALLS[[r]][-1]);a<-a[a<nrow(ads)];
		i <- as.integer(ACTIONS[[r]][-1]);i<-i[i<nrow(packages)];
		target[i,a] <- target[i,a] + 1;

		if(r %% 100000 == 0){print(paste("生成",method,"矩阵完成",round(100*r/length(INSTALLS),2),"%",sep=""));}#输出进度
	}
	write.table(target,paste(loc,method,"/character_matrix",sep=""),append=FALSE,row.names=TRUE,col.names=TRUE,sep=",");
	return(target);
}

generate_NMF_matrix <- function(v, w, max.iteration=200){
	w <- as.matrix(w);v <- as.matrix(v);
	h <- matrix(sample(ncol(w)*ncol(v)) / (ncol(w)*ncol(v)),nr=ncol(w),nc=ncol(v));
	w <- w + 0.0000000001;
	v <- v + 0.0000000001;
	ptm<-proc.time();
	for(i in 1:max.iteration){
		wh <- w %*% h;
		cost <- sum((v-wh)^2);
		if(i %% 1 == 0){
			print(paste("1 iteration cost = ",cost,", time used ",round((proc.time() - ptm)[3],2),"s!",sep=""));
			ptm<-proc.time();
			write.table(h,paste(loc,method,"/NMF_matrix",sep=""),append=FALSE,row.names=TRUE,col.names=TRUE,sep=",");
		}
		if(cost < 0.001){break;}
		hn <- t(w) %*% v;
		hd <- (t(w) %*% w) %*% h;
		h <- h * hn / hd;
		
	}
	return(h)
}

recommend_for_users <- function(sour,matr,meth,INDICATOR=FALSE,output_file=""){
	if(INDICATOR){indicators <- c();}
	count <- 0;
	ptm<-proc.time();
	for(x in 1:length(sour)){
		user <- names(sour[x]);apps <- as.integer(sour[[x]]);
		if(meth == "itemCF"){
			apps <- apps[apps <= no_of_ads];
			m <- matrix(0,nr=1,nc=no_of_ads);m[apps] <- 1;
			rec_list <- matr[as.integer(rownames(ads)),] %*% t(m);
		}else if(meth == "character" | meth == "NMF"){
			m <- matrix(0,nr=1,nc=nrow(packages));m[apps] <- 1;
			rec_list <- t(m %*% matr[,as.integer(rownames(ads))]);
		}

		#rec_list <- rec_list / factor;#除法

		if(max(rec_list) != 0){
			rec_list <- rec_list / max(rec_list);

			if(INDICATOR){
				indi <- rec_list[as.integer(ACTIONS[[user]][-1]),];
				indi <- mean(indi);
				indicators <- append(indicators,indi);
			}
		}
		rec_list <- names(rec_list[order(rec_list,decreasing=TRUE),]);
		output <- paste(user,connectwithcomma(rec_list),sep=",");

		if(output_file != ""){
			write.table(output,paste(loc,method,"/",output_file,sep=""),append=TRUE,quote=FALSE,row.names=FALSE,col.names=FALSE);
		}else{
			write.table(output,paste(loc,method,"/",meth,"_rec",sep=""),append=TRUE,sep=",",quote=FALSE,row.names=TRUE,col.names=FALSE);
		}
		count <- count + 1;
		if(count %% 10000 == 0){
			print(paste(meth,"为每个用户形成推荐完成",round(100*count/length(sour),2),"%,用时",round((proc.time() - ptm)[3],2),"s!",sep=""));
			ptm<-proc.time();
		}#输出进度
	}

	if(INDICATOR){
		indicator_result <- paste("Indicator: average = ",round(mean(indicators) * 100,2),"%,sd = ",round(sd(indicators) * 100,2),"%",sep="");
		write.table(indicator_result,paste(loc,meth,"/",meth,"_indicator",sep=""),append=FALSE,sep=",",quote=FALSE,row.names=FALSE,col.names=FALSE);
	}
}

make_ACTIONS_INSTALLS <- function(source,id){
	ACTIONS <- matrix(0,nr=0,nc=nrow(ads));
	INSTALLS <- matrix(0,nr=0,nc=nrow(packages));
	for(r in 1:nrow(source)){
		tmp <- source[r,];rownames(tmp) <- tmp[,id];tmp <- tmp[-1];#rownames(tmp) <- sapply(strsplit(tmp[,id],"ei."),"[",2);
		a <- tmp[1:nrow(ads)];a<-as.matrix(a);
		i <- tmp[nrow(ads)+1];rm(tmp);col <- packagetoid(strsplit(i[,"p"],",")[[1]],packages);tmp <- matrix(0,nr=1,nc=nrow(packages),dimnames=list(source[r,id],c(1:nrow(packages))));for(c in col){tmp[,c] <- 1;};i<-tmp;
		ACTIONS <- check_and_append(ACTIONS,a)
		INSTALLS <- check_and_append(INSTALLS,i)
		if(r %% 1000 == 0){print(paste("安装&广告矩阵生成，progress ",100*r/nrow(source),"%",sep=""));}#输出进度
	}
	return(list(ACTIONS,INSTALLS));
}

check_and_append <- function(m,r){
	if(length(m[rownames(m) == rownames(r),]) > 0){
		m[rownames(r),] <- m[rownames(r),] | r;
	}else{
		m <- rbind(m,r);
	}
	return(m);
}

#把youmi转换成含有分类的target
format_to_category <- function(sour, list){
	target <- matrix(,nc=0,nr=row);
	for(i in 1:length(list)){
	  tmp_data <- as.matrix(apply(sour[,tags[,2] == i,drop=FALSE],1,sum));
	  target <- cbind(target,tmp_data);
	}
	rm(i, tmp_data);
	colnames(target) <- cat_list;
	target <- as.data.frame(target);
	return(target);
}
#把youmi转换成含有分类的target

from_cat_to_pac <- function(sour,tags){
	target <- sour;
	target[] <- 0;
	for (r in 1:nrow(sour)){
		for (c in 1:ncol(sour)){
			c_list <- unique(tags[tags[,3] == sour[r,c],c(1,4)]);
			c_list <- c_list[order(c_list[,2],na.last = NA,decreasing=T),];
			c_list[,2] <- c_list[,2] / apply(c_list[,2,drop=F],2,sum);
			#未去重
			pos <- sample.int(nrow(c_list), size = 1, replace = FALSE, prob = c_list[,2]);
			target[r,c] <- c_list[pos,1];
			}
	}
	return(target);
}


PRF_none <- function(rec,sour){
	index <- matrix(,nr=nrow(rec),nc=3,dimnames=list(rownames(rec),c("Precision","Recall","F-Score")));
		if(nrow(rec) == nrow(sour)){
			for (r in 1:nrow(rec)){		
				A = length(sour[r,as.vector(as.matrix(rec[r,]))][,sour[r,as.vector(as.matrix(rec[r,]))]>0]);
				B = length(rec[r,]) - A;
				C = length(sour[r,sour[r,] > 0]) - A;
				prec = A / (A + B);
				reca = A / (A + C);
				if(prec == 0 & reca == 0){
					F = 0;
				}else{
					F = (2 * prec * reca) / (prec + reca);
				}
				index[r,] <- c(prec, reca, F);
				if(r %% 1000 == 0){print(paste("计算Precision&Recall&F-Score完成",100*r/nrow(rec),"%",sep=""));}#输出进度
			}
		return(apply(index,2,mean));
	}
}

install_list_to_matrix <- function(source,packages){
	ps <- strsplit(as.matrix(source),",");
	cols<-c();
	for(p in ps){cols<-append(cols,packagetoid(p,packages));}
	cols<-unique(cols);
	i <- matrix(0,nr=1,nc=nrow(packages),dimnames=list(rownames(one),c(1:nrow(packages))));
	i[cols]<-1;
	return(i);
}

getIAasList <- function(ia,ROW=-1,rip_first=FALSE){
	target <- scan(paste(loc,"lib/",ia,sep=""),what=character(0),nlines=ROW,sep="");
	target <- as.list(target);
	target <- strsplit(sapply(target,"[",1),",");
	names(target) <- sapply(target,"[",1);
	if(rip_first){
		target <- rip_list_first(target);
	}
	return(target);
}

listtomatrix <- function(l,link){
	ptm<-proc.time();
	target <- matrix(0,nr=0,nc=nrow(link));
	colnames(target) <- c(1:nrow(link));
	for(x in l){
		user <- x[1];x <- as.integer(x[-1]);
		m <- matrix(0,nr=1,nc=nrow(link));
		m[1,x] <- 1;
		target <- rbind(target,m);
	}
	print(paste("ACTIONS&INSTALLS的矩阵构建完成,用时",round((proc.time() - ptm)[3],2),"s",sep=""));
	return(target);
}

cut_log <- function(log,cut,data,folder,cut_to=""){
	#从cut_to开始
	if(cut_to != ""){
		cut_to_number <- max(which(log[1]==cut_to));
		log <- log[-c(1:cut_to_number),];
	}
	#从cut_to开始
	#cut log to n pieces
	count <- 1;
	system(paste("rm -r ",loc,"lib/tmp/",folder,sep=""));
	system(paste("mkdir ",loc,"lib/tmp/",folder,sep=""));
	repeat{
		if(cut < nrow(log)){
			repeat{
				if(is.na(log[cut+1,1]) | log[cut,1] != log[cut+1,1]){break;}
				cut <- cut + 1;
			}
			write.table(log[c(1:cut),],paste(loc,"lib/tmp/",folder,"/ordered_",data,"_",count,sep=""),append=FALSE,sep=",",quote=FALSE,row.names=FALSE,col.names=FALSE);
			log <- log[-c(1:cut),];
			count <- count + 1;
		}else{
			if(nrow(log) > 0){
				write.table(log,paste(loc,"lib/tmp/",folder,"/ordered_",data,"_",count,sep=""),append=FALSE,sep=",",quote=FALSE,row.names=FALSE,col.names=FALSE);
			}
			return(count);
			break;
		}
	}
	print("Cut end!");
	#cut log to n pieces
}

rip_list_first <- function(lists){
    for(i in 1:length(lists)){
            col_name <- names(lists[i]);
            lists[[i]]<-lists[[i]][-1];
    }
    return(lists);
}

trans <- function(lists){
	target <- as.list(matrix(,nr=1283,nc=1));
	names(target) <- c(1:1283);
	for(l in 1:length(lists)){
		user <- names(lists[l]);
		for(i in as.integer(lists[[l]])){
			target[[i]] <- append(target[[i]],user)
		}
	}
	target <- target[!is.na(sapply(target,"[",2))];
	return(rip_list_first(target));
}

generate_NMF_matrix_in_list <- function(w,v, max.iteration=200,rip_first=FALSE){
	#去除user占了第一个位置
	if(rip_first){
		w <- rip_list_first(w);v <- rip_list_first(v);
	}
	#去除user占了第一个位置

	h <- matrix(sample(nrow(packages)*no_of_ads) / (nrow(packages)*no_of_ads),nr=nrow(packages),nc=no_of_ads);
	cost <- 0;
	ptm<-proc.time();
	for(iter in 1:max.iteration){
		#计算INSTALLS和NMF的乘
		wh <- matrix(0,nr=length(w),nc=no_of_ads);
		for(i in 1:length(w)){
			i_install <- as.integer(w[[i]]);
			for(j in 1:ncol(h)){
				wh[i,j] <- sum(h[i_install,j]);
			}
		}
		#计算INSTALLS和NMF的乘

		#计算cost和slope
		last_cost <- cost;
		for(i in 1:length(v)){
			i_action <- as.integer(v[[i]]);
			cost <- sum((wh[i,i_action] - 1)^2) + sum((wh[i,-i_action])^2);
		}
		slope <- abs((last_cost-cost)/cost);
		#计算cost和slope

		#终止条件
		if(iter %% 1 == 0){
			print(paste("1 iteration cost = ",cost,", time used ",round((proc.time() - ptm)[3],2),"s!",sep=""));
			ptm<-proc.time();
			write.table(h,paste(loc,method,"/NMF_matrix",sep=""),append=FALSE,row.names=TRUE,col.names=TRUE,sep=",");
		}
		if(cost < 0.00001 | slope < 0.000001){break;}
		#终止条件

		#重新构建h
			#hn <- t(w) %*% v;
			hn <- matrix(0,nr=nrow(packages),nc=no_of_ads);
			t_w <- trans(w);
			t_v <- trans(v);
			for(i in 1:length(t_w)){
				r <- as.integer(names(t_w[i]));
				for(j in 1:length(t_v)){
					c <- as.integer(names(t_v[j]));
					hn[r,c] <- length(intersect(t_w[[i]],t_v[[j]]));
				}
			}
			#hn <- t(w) %*% v;

			#hd <- (t(w) %*% w) %*% h;
			hd <- matrix(0,nr=nrow(packages),nc=no_of_ads);

			t_w_x_w <- matrix(0,nr=nrow(packages),nc=nrow(packages))
			t_w <- trans(w);
			for(i in 1:length(t_w)){
				r <- as.integer(names(t_w[i]));
				for(j in 1:length(t_w)){
					c <- as.integer(names(t_w[j]));
					t_w_x_w[r,c] <- length(intersect(t_w[[i]],t_w[[j]]));
				}
			}

			hd <- t_w_x_w %*% h + 0.00000001;

			#hd <- (t(w) %*% w) %*% h;

			#h <- h * hn / hd;
			h <- h * hn / hd;
			#h <- h * hn / hd;
		#重新构建h
		
	}
	dimnames(h) <- list(c(1:nrow(h)),c(1:ncol(h)));
	return(h)
}

list_to_matrix <- function(sour){
	target <- matrix(,nr=0,nc=nrow(category));colnames(target) <- c(1:nrow(category))
	for(i in 1:length(sour)){
		user <- names(sour[i]);
		ias <- as.integer(sour[[i]]);

		m <- matrix(0,nr=1,nc=nrow(category),dimnames=list(user,c(1:nrow(category))));
		for(ia in ias){
			m[ia] <- m[ia] + 1;
		}
		target <- rbind(target,m);
	}
	return(target)
}

log_to_action <- function(data,user,formmatrix){
	data <- as.vector(as.matrix(data));
	a<-unique(data);

	if(formmatrix){
		target <- matrix(0,nr=1,nc=nrow(ads));
		target[1,a] <- 1;
	}else{
		target <- matrix(connectwithcomma(a),nr=1,nc=1);
	}
	rownames(target) <- user;
	return(target);
}
log_to_install <- function(data,user,formmatrix){
	i <- unique(as.vector(data));i<-i[i != ""];
	i <- packagetoid(i,packages);

	if(formmatrix){
		target <- matrix(0,nr=1,nc=nrow(packages));target[1,i] <- 1;
	}else{
		target <- matrix(connectwithcomma(i),nr=1,nc=1)
	}
	rownames(target) <- user;
	return(target);
}

load_and_store <- function(log,result,formmatrix){
	if(formmatrix){file_name_tip <- "_m"}else{file_name_tip <- ""}
	if(result == "ACTIONS"){
		user <- as.character(log[1,1]);
		end <- 1;
		repeat{
			end <- end +1;
			next_user <- as.character(log[end,1]);
			if(is.na(log[end,1]) | (next_user != user)){end<-end-1;break;}
		}
		write.table(log_to_action(log[c(1:end),2],user,formmatrix),paste(loc,"lib/",result,"_raw",file_name_tip,sep=""),append=TRUE,sep=",",quote=FALSE,row.names=TRUE,col.names=FALSE);
		
	}else if(result == "INSTALLS"){
		user <- log[[1]][1];
		end <- 1;
		repeat{
			end <- end +1;
			if(end > length(log)){end<-end-1;break;}
			next_user <- log[[end]][1];
			if(next_user != user){end<-end-1;break;}
		}
		write.table(log_to_install(unlist(sapply(log[1:end],"[",-1)),user,formmatrix),paste(loc,"lib/",result,"_raw",file_name_tip,sep=""),append=TRUE,sep=",",quote=FALSE,row.names=TRUE,col.names=FALSE);
	
	}

	return(end);
}

transform_log_to_a <- function(data,result,formmatrix,cut_to=""){
	ptm<-proc.time();
	log <- read.table(paste(loc,"lib/",data,sep=""),header=FALSE,sep=",",nrow=ROW,na.strings = "NA",fill=TRUE);

	log <- log[c(2,3)];log<-log[!is.na(log[2]),];log<-log[order(log[1]),,drop=FALSE];
	print(paste("Load ",(proc.time() - ptm)[3]," s!",sep=""));
	
	file_count <- cut_log(log,a_cut,data,folder=data,cut_to);

	for(i in 1:file_count){
		log <- read.table(paste(loc,"lib/tmp/",data,"/ordered_",data,"_",i,sep=""),header=FALSE,sep=",",nrow=-1,na.strings = "NA",fill=TRUE);
		ptm<-proc.time();
		repeat{
			if(nrow(log) == 0){break;}
			end <- load_and_store(log,result,formmatrix)
			log <- log[-c(1:end),];
		}
		print(paste("The ",i," of ",file_count," Finished, used ",round((proc.time() - ptm)[3],2),"s!",sep=""));
	}
}

transform_log_to_i <- function(data,result,formmatrix,cut_to=""){

	ptm<-proc.time();

	log <- rbind(read.csv(paste(loc,"lib/",data,"_1.csv",sep=""),header=TRUE,sep=",",nrow=-1),read.csv(paste(loc,"lib/",data,"_2.csv",sep=""),header=TRUE,sep=",",nrow=-1));
	log[2]<-sapply(strsplit(as.matrix(log[2]),"ei."),"[",2);

	log <- log[c(2,3)];log<-log[!is.na(log[2]),];log<-log[order(log[1]),,drop=FALSE];
	log[2] <- gsub(pattern = "iphone identifier", replacement = "iphone.identifier", gsub(pattern = " ,", replacement = ",", gsub(pattern = " name ", replacement = ".name.", gsub(pattern = " ,", replacement = ",", as.matrix(log[2])))));

	print(paste("Load ",(proc.time() - ptm)[3]," s!",sep=""));
	
	file_count <- cut_log(log,i_cut,data,cut_to);


	for(i in 1:file_count){
		log <- read.table(paste(loc,"lib/tmp/",data,"/ordered_",data,"_",i,sep=""),header=FALSE,sep="",nrow=-1,na.strings = "NA",fill=TRUE);
		log <- strsplit(as.matrix(log[1]),",");
		ptm<-proc.time();
		repeat{
			if(length(log) == 0){break;}
			end <- load_and_store(log,result,formmatrix);
			log <- log[-c(1:end)];
		}
		print(paste("The ",i," of ",file_count," Finished, used ",round((proc.time() - ptm)[3],2),"s!",sep=""));
	}
}

recommend_by_category <- function(resource,USER){
	ptm<-proc.time();
	IA <- get(resource);
	for(i in 1:length(IA)){
		if(i %% 10000 == 0){print(paste(USER,", progress ",round(100*i/length(IA),2),"%",sep=""));}#输出进度
    	user <- names(IA[i]);
    	ia <- as.integer(IA[[i]]);
    	if(resource == "ACTION"){
    		cat <- old.ads[ia,'category'];cat <- cat[!is.na(cat)];
		}else if(resource == "INSTALL"){
			cat <- packages[ia,'category'];cat <- cat[!is.na(cat)];
		}

		m<-matrix(0,nr=1,nc=nrow(category),dimnames=list('cat',c(1:nrow(category))));
	    for(j in 1:nrow(category)){
	            m[1,j] <- m[1,j] + length(which(cat == j));
	    }

	    m <- m[,order(m,decreasing=TRUE),drop=FALSE];

	    #if(length(colnames(m[,m>0,drop=FALSE])) == 0){next}

	    output <- c();
	    for(c in as.integer(colnames(m[,m>0,drop=FALSE]))){
	        apps <- as.integer(rownames(ads[ads['category'] == c,]));
			output <- append(output,rev(apps));
	    }
	    output <- append(output,setdiff(new_to_old_ads,output));

	    write.table(connectwithcomma(append(user,output)),paste(method,"/",USER,sep=""),append=TRUE,quote=FALSE,row.names=FALSE,col.names=FALSE);

	}
	print(paste("给",USER,"推荐app完成,用时",round((proc.time() - ptm)[3],2),"s",sep=""));
}

assert_no_replicate <- function(groups){
	USERS <- c();
	for (g in groups){
		users <- scan(paste(method,"/",g,sep=""),what=character(0),sep="");
		users <- sapply(strsplit(users,","),"[",1);
		USERS <- append(USERS,users);
	}
	
	return(length(USERS) - length(unique(USERS)));

}