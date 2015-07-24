from log_analysis.class_log import class_log

if __name__ == '__main__':
    obj = class_log('./log_dir/q19.sql.log','q19.xml')
    obj.getAttri()
    obj.writeToXML()