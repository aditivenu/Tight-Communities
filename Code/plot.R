#To plot clustering coefficient vs no. of connected components for facebook dataset
#Read the file
fb = read.table(file.choose(), header=T, sep="\t") 
#Display the contents of the file
fb
#Plot the contents
plot(fb, col="blue",xlab = "Clustering Coefficient", ylab = "No. of connected components")
lines(fb$CCT,fb$NCC, col="green")

#To plot clustering coefficient vs no. of connected and strongly connected components for twitter dataset
#Read the file
twit = read.table(file.choose(), header=T, sep="\t") 
#Display the contents of the file
twit
#Plot the file
plot(twit$Coeff,twit$sconn,type="l",col="red",xlab = "Clustering Coefficient", ylab = "No. of connected components")
lines(twit$Coeff,twit$conn,type="l",col="blue")
#To display the legends
legend(0.55, 30000, legend=c("strongly connected components","connected components"), col=c("red","blue"), lty=1:2, cex=0.6)

#To display how part of the facebook dataset looks
#Read the dataset
fbdata = read.table(file.choose(), header=T, sep=" ")
#Plot the dataset
library("networkD3")
simpleNetwork(fbdata)

#To display how part of the twitter dataset looks
#Read the dataset
twitdata = read.table(file.choose(), header=T, sep=" ")
#Plot the dataset
library("networkD3")
simpleNetwork(twitdata)
