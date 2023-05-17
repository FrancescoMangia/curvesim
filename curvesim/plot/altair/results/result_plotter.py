from altair import TitleParams, vconcat

from curvesim.utils import override
from curvesim.plot.result_plotter import ResultPlotter
from .make_page import make_page_from_results


def plot_results(results):
    summary = plot_summary(results)
    timeseries = plot_data(results)
    return vconcat(summary, timeseries).resolve_scale(color="independent")


def plot_summary(results):
    title = TitleParams(text="Summary Metrics", fontSize=16)
    data_key = "summary"
    axes = {"metric": "y", "dynamic": {"x": "x:Q", "color": "color:O"}}

    page = make_page_from_results(results, data_key, axes)
    return page.properties(title=title)


def plot_data(results):
    title = TitleParams(text="Timeseries Data", fontSize=16)
    data_key = "data"
    axes = {"metric": "y", "dynamic": {"color": "color:O"}}

    page = make_page_from_results(results, data_key, axes, downsample=True)
    return page.properties(title=title)


class AltairResultPlotter(ResultPlotter):
    @override
    def save(self, plot, save_as):
        plot.save(save_as)


result_plotter = AltairResultPlotter(plot_data, plot_results, plot_summary)
