from fastavro import reader as avro_reader
import tarfile

tar = tarfile.open('data/openintel-umbrella1m-20200101.tar', mode='r')
aaaa = False
c_name = False
a = False
for name in tar.getnames():
    if name == 'CO_0C279507D0567E9288E66C99FF467818.avro':
        avro_io = avro_reader(tar.extractfile(name))
        for record in avro_io:
            if record['response_type'] == 'AAAA' and not aaaa:
                print(type(record), record)
                aaaa = True
            elif record['response_type'] == 'A' and not a:
                print(type(record), record)
                a = True
            elif record['response_type'] == 'CNAME' and not c_name:
                print(type(record), record)
                c_name = True
            else:
                if c_name and a and aaaa:
                    exit()
