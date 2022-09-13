from logging import exception
import subprocess

class AwsOperations():
    def __init__(self):
        pass

    def get_put_file_s3(self, source_filepath, dest_file_path):
        try:
            cmd = 'aws s3 cp ' + source_filepath + ' ' + dest_file_path
            print(cmd)
            pipes = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            std_out, std_err = pipes.communicate()
            if pipes.returncode != 0:
                err_msg = pipes.returncode
                print(err_msg)
                print(std_out)
                print(std_err)
                return False
            elif len(std_err):
                print(std_err)
            return True

        except exception as e:
            print(e)
        

    def validate_s3_file_path(self, s3_file_path):
        '''
        Validates if given S3 file path is valid
        '''
        try:
            valid_file_types = ['.tsv']
            cmd = 'aws s3 ls ' + s3_file_path
            print(cmd)
            pipes = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            std_out, std_err = pipes.communicate()
            if pipes.returncode != 0:
                err_msg = pipes.returncode
                print(err_msg)
                print(std_out)
                print(std_err)
                return False
            elif len(std_err):
                print(std_err)
                return False
            else:
                str_list = s3_file_path.split('/')
                file_list = str_list[-1:]
                if file_list[0][-4:] in valid_file_types:
                    return True
                else:
                    return False
        except exception as e:
            print(e)


if __name__ == "__main__":

    aws_ops = AwsOperations()
    aws_ops.get_put_file_s3(source_filepath='s3://revenue-analyzer-file-store/input/data_1.tsv', dest_file_path='/home/ubuntu/input')
