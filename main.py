from log_analysis.class_log import class_log
import logging
logging.basicConfig(
                    level=logging.DEBUG,
                    format="%(asctime)s %(filename)s %(levelname)s %(message)s",
                    datefmt="%a,%d %b %Y %H:%M:%S",
                    filename='mylog.log',
                    filemode='w')

if __name__ == '__main__':
    obj = class_log('./log_dir/q19.sql.log','q19.xml',logging)
    obj.getAttri()
    obj.writeToXML()