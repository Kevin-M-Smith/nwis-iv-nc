test:
	time ./bootstrapTest.R

bootstrap:
	time ./bootstrap.R

buildall:
	mkdir -p out
	time ./buildAll.R

buildall2:
	mkdir -p out2
	time ./buildAll2.R

destroy:
	time ./destroyTables.R

setup:
	time ./buildTables.R

install:
	time ./installer.R
