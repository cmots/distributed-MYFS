all:
	mkdir -p mountdir
	mkdir -p rootdir
	mkdir -p meta
	echo "bogus file" > rootdir/bogus.txt

distdir:
	cp Makefile $(distdir)

mostlyclean clean distclean mainainer-clean:
	rm -r mountdir rootdir meta bbfs.log *.test
