local files : dir "C:\Users\dnratnadiwakara\Documents\sunkcost_2019\ztraxdata\raw\36\ZTrans" files "*.*"

cd "C:\Users\dnratnadiwakara\Documents\sunkcost_2019\ztraxdata\raw\36\ZTrans"

foreach file in `files' {
	clear all
	cd "C:\Users\dnratnadiwakara\Documents\sunkcost_2019\ztraxdata\raw\36\ZTrans"
	import delimited `file', delimiter("|") clear
	export delimited using `file', delimiter("|") nolabel datafmt replace
}
