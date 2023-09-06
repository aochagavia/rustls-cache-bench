#!/usr/bin/env python
# coding: utf-8

# In[15]:


base_path = '/path/to/rustls-cache-bench/bench_out'
fh_subpath = 'fh'
rh_subpath = 'rh'

def read_handshake_durations(path):
    raw_handshakes = None
    with open(path, 'r') as file:
        raw_handshakes = file.read()

    split = raw_handshakes.split(';')
    split.pop()
    return [int(x) for x in split]

def read_all_handshake_durations(cache_size):
    return (cache_size,
            read_handshake_durations(os.path.join(base_path, cache_size, fh_subpath)),
            read_handshake_durations(os.path.join(base_path, cache_size, rh_subpath)))


# In[22]:


import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300

def plot_some(data, title):
    n_bins = 50
    fig, ax = plt.subplots(figsize=(8, 4))

    for (cache_size, fh, rh) in data:
        fh_and_rh = fh + rh
        n, bins, patches = ax.hist(fh_and_rh, n_bins, density=True, histtype='step', cumulative=False, label=f'Handshake ({cache_size})')
        # n, bins, patches = ax.hist(rh, n_bins, density=True, histtype='step', cumulative=False, label=f'Resumed handshake ({cache_size})')

    ax.grid(True)
    ax.legend(loc='right')
    ax.set_title(title)
    ax.set_xlabel('Handshake duration (µs)')
    ax.set_ylabel('Likelihood of occurrence')

    plt.show()

def plot_one(cache_size):
    data = [read_all_handshake_durations(cache_size)]
    plot_some(data, f'Results')

def plot_two(cache_size1, cache_size2):
    data = [read_all_handshake_durations(cache_size) for cache_size in [cache_size1, cache_size2]]
    plot_some(data, f'Cache sizes {cache_size1} and {cache_size2}')


# In[27]:


import os

# cache_sizes = [path for path in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, path)) and os.path.isfile(os.path.join(base_path, path, fh_subpath))]
# cache_sizes = sorted(cache_sizes, key=lambda cache_size: int(cache_size))

# cache_sizes = ['1000', '2000']
# print(cache_sizes)

# window_size = 2
# windows = [cache_sizes[i : i + 2] for i in range(len(cache_sizes) - 1)]

# for [cache_size_1, cache_size_2] in windows:
#     plot_two(cache_size_1, cache_size_2)

# plot_two('1000', '2000')
plot_one('10000')


# In[28]:


import json

throughput_path = os.path.join(base_path, 'throughput.json')
throughputs = None
with open(throughput_path, 'r') as f:
  throughputs = json.load(f)

cache_sizes = [t['cache_size'] for t in throughputs]
requests_per_second = [t['requests_per_second'] for t in throughputs]
saturation = [(t['fh_server_side_seconds'] + t['rh_server_side_seconds']) / (t['fh_client_side_seconds'] + t['rh_client_side_seconds']) * 100 for t in throughputs]

fig, ax1 = plt.subplots()

color = 'tab:blue'
ax1.set_xlabel('Cache size')
ax1.set_ylabel('Req/s')
ax1.plot(cache_sizes, requests_per_second)
ax1.tick_params(axis='y', labelcolor=color)

color = 'tab:red'
ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
ax2.set_ylabel('Saturation (%)')  # we already handled the x-label with ax1
ax2.plot(cache_sizes, saturation, color=color)
ax2.tick_params(axis='y', labelcolor=color)

fig.tight_layout()  # otherwise the right y-label is slightly clipped

plt.show()
# plt.savefig('filename.png', dpi=300)


# In[ ]:




