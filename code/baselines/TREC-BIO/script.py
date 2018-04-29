import subprocess

sim_list = ['tfidf', 'bm25', 'lm_jm', 'lm_dir', 'ib']
mode_list = ['both', 'word', 'entity']

for sim in sim_list:
	for mode in mode_list:
		cmd_temp = 'python3 search_data.py -sim {} -mode {}'.format(sim, mode)
		subprocess.call(cmd_temp, shell = "True")
