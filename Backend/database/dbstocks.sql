use stock_prediction;

CREATE TABLE users(
	userid varchar(50) not null primary key ,
    username varchar(20) not null,
    password varchar(30) not null,
    email varchar(50) not null unique key,
    fullname varchar(30) not null ,
    type varchar(15) not null,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
CREATE TABLE stocklist(
	stockid varchar(50) not null primary key ,
    symbol varchar(10) not null unique key,
    company_name varchar(60) not null ,
    company_detail varchar(200) ,
    previous_close_price double not null
);

CREATE TABLE comments(
commentid varchar(50)  not null primary key ,
userid varchar(50) not null ,
stockid varchar(50) not null,
comment_text varchar(200),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
foreign key (userid) references users(userid),
foreign key (stockid) references stocklist(stockid)
);
CREATE TABLE StockFollow (
followid varchar(50) not null primary key  ,
userid varchar(50) not null,
stockid varchar(50) not null ,
foreign key (userid) references users(userid),
foreign key (stockid) references stocklist(stockid)

);
CREATE TABLE stockhistory(
id varchar(50) not null primary key ,
stockid varchar(50) not null  ,
date datetime not null ,
open double not null,
high double not null,
low double not null,
close double not null,
vollumn double not null,
foreign key (stockid) references stocklist(stockid)
);
CREATE TABLE stockprediction(
predictionid varchar(50) not null  primary key ,
stockid varchar(50) not null ,
userid varchar(50) not null,
date datetime not null ,
text_prediction varchar(300) not null,
foreign key (userid) references users(userid),
foreign key (stockid) references stocklist(stockid)
)