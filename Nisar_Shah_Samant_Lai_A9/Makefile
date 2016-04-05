lr:
	cd nisar_shah_samant_lai_a8 && exec mvn clean install
	pwd
	cp nisar_shah_samant_lai_a8/target/nisar_shah_lai_samant_a8-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	cd performance && exec mvn clean install
	cp performance/target/uber-perf-0.0.1.jar .
	java -cp uber-perf-0.0.1.jar neu.perf.App -kind=cloud -arguments="-time=${TIME} $(INPUT_PATH) $(OUTPUT_PATH)"
	sudo apt-get install texlive-latex-extra
	sudo apt-get install pandoc
	# call to generate grapth in R
	sudo Rscript -e "install.packages('knitr', repos = c('http://yihui.name/xran', 'http://cran.rstudio.org')); install.packages('rmarkdown', repos='http://cran.rstudio.com/');install.packages('ggplot2', repos='http://cran.rstudio.com/');install.packages('lubridate', repos='http://cran.rstudio.com/');install.packages('plyr', repos='http://cran.rstudio.com/');require(knitr); require(markdown); rmarkdown::render('output.Rmd')"
	sudo nohup xdg-open output.pdf &
