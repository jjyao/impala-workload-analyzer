import numpy
from matplotlib import cm
from matplotlib import pyplot

outputDir = None

def scatter(data, xlabel, ylabel, title, output):
    global outputDir

    pyplot.clf()
    pyplot.scatter(range(0, len(data)), data)
    pyplot.xlim(0)
    pyplot.ylim(0)
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)
    pyplot.title(title)
    pyplot.tight_layout()
    pyplot.savefig('%s/%s' % (outputDir, output))

def hist(data, minval, maxval, xlabel, ylabel, title, output, **kwargs):
    global outputDir
    ylog = kwargs.get('ylog', False)

    pyplot.clf()
    min_num_bins = 10
    max_num_bins = 10
    step = max(1, (maxval - minval) / max_num_bins)
    start = minval
    stop = max(start + step * (min_num_bins + 1), maxval + step)
    if isinstance(minval, int):
        bins = range(start, stop, step)
    else:
        bins = numpy.arange(start, stop, step)
    pyplot.hist(data, bins, log=ylog)
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)
    pyplot.title(title)
    pyplot.tight_layout()
    pyplot.savefig('%s/%s' % (outputDir, output))

def bar(data, minval, maxval, xlabel, ylabel, title, output, **kwargs):
    global outputDir
    ylog = kwargs.get('ylog', False)

    pyplot.clf()
    height = [0] * (maxval + 1 - minval)
    for i in data:
        height[i - minval] += 1
    left = numpy.arange(minval, maxval + 1)
    pyplot.bar(left, height, align = 'center', log=ylog)
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)
    pyplot.xticks(left)
    pyplot.title(title)
    pyplot.tight_layout()
    pyplot.savefig('%s/%s' % (outputDir, output))

def stacked_bar(data, ylabel, legends, title, output):
    global outputDir

    pyplot.clf()
    bars = []
    left = numpy.arange(1)
    y_offset = 0
    for i in xrange(len(data)):
        bars.append(pyplot.bar(left, (data[i],), align = 'center', bottom = (y_offset,), color=cm.jet(1.0 * i / len(data))))
        y_offset += data[i]
    pyplot.ylabel(ylabel)
    pyplot.xticks(left)
    pyplot.legend((bar[0] for bar in bars), legends, loc='center right', bbox_to_anchor=(0.1, 0.5))
    pyplot.title(title)
    pyplot.savefig('%s/%s' % (outputDir, output), bbox_inches='tight')

def pie(data, legends, title, output):
    global outputDir

    pyplot.clf()
    patches, texts = pyplot.pie(data)
    legends = ['{0} - {1:1.2f} %'.format(label, pct * 100) for label, pct in zip(legends, data)]
    pyplot.legend(patches, legends, loc='center right', bbox_to_anchor=(0.1, 0.5))
    pyplot.title(title)
    pyplot.savefig('%s/%s' % (outputDir, output), bbox_inches='tight')
