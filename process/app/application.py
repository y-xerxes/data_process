"""
在整体的数据体系中, 做以下基层抽象:

1. Application:
  应用, 面向实际运行的应用, 如:
  * StreamApp: 实时运算的App
  * DailyRetailerCalculateApp: 每个商户每日运算应用

2. Buz:
  业务, 如:
  * 导购屏: guider_screen
  * 商户经营数据: retailer_statistics

  业务作为一组:
  1. Loader: 从数据库中或者其它方式中获取数据的机制
  2. Calculator: 算法, 根据输入的RDD或者DF, 算出另一个DF的过程, 从中不载入数据
  3. DataFetcher: 面向Cache机制的数据获取机制, 从中会调用Loader
  4. Stage: 从App的上下文中获取一些Stage之间共享的上下文, 来完成业务的某一个场景的运算要求
     比如:
     * 面向商户的商品计算商品维度的相关数据
"""
from process.util.data_context import DataContext

class DataApplication(object):
    def __init__(self, data_context):
        # type: (DataContext) -> None
        super(DataApplication, self).__init__()
        self.data_context = data_context

    def execute(self, **kwargs):
        pass
