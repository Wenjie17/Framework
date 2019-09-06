from Engine.Algorithm import *
from Engine.Parameters import *
from Engine.Calender import *
from Engine.Environment import *


def Analyze(api, context, results):
    print("Analyze")
    print(results.head())
    name = api._name
    pathFilename = "d:/Data/Strategy/" + name
    Gadget.CreateFolder(pathFilename)

    results.to_csv(pathFilename + '/performances.csv')

    import matplotlib.pyplot as plt
    # Plot the Portfolio Return and Excess Return data.
    #
    ax1 = plt.subplot(211)
    results.UnitNetValue.plot(ax=ax1)
    results.Benchmark.plot(ax=ax1)
    ax1.set_ylabel('Net Unit Value')
    # ax1.set_xlabel('DateTime')
    #
    # ax2 = plt.subplot(212, sharex=ax1)
    ax2 = plt.subplot(212)
    results.CumExcessReturn.plot(ax=ax2)
    ax2.set_ylabel('Cumulative Excess Return')

    # Show the plot.
    # plt.gcf().set_size_inches(18, 8)
    plt.savefig(pathFilename + "/performances.jpg")
    plt.show()
    pass