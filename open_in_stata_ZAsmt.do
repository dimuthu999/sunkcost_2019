local files : dir "C:\Users\dnratnadiwakara\Documents\sunkcost_2019\ztraxdata\raw\Historical\36" files "*.*"

cd "C:\Users\dnratnadiwakara\Documents\sunkcost_2019\ztraxdata\raw\Historical\36"

foreach file in `files' {
	clear all
	cd "C:\Users\dnratnadiwakara\Documents\sunkcost_2019\ztraxdata\raw\Historical\36"
	import delimited `file', delimiter("|") clear
	export delimited using `file', delimiter("|") nolabel datafmt replace
}
