import npyscreen
global file_path,num_workers,format_type,model_type,iplist,program_path

class ApatheticML(npyscreen.Form):
    def create(self):
        self.m = ['K Means Clustering','Linear Regression','Logistic Regression','Random Forest']
        self.splits  = self.add(npyscreen.TitleText,name='Enter the no. of workers:')
        self.model = self.add(npyscreen.TitleSelectOne, scroll_exit=True, max_height=5, name='Model:', values = self.m)
        self.file = self.add(npyscreen.TitleFilenameCombo,scroll_exit=True,max_height = 5,name='File choice: ')    
        self.format = self.add(npyscreen.TitleSelectOne, scroll_exit=True,max_height = 5,name='Data format: ',values=['.csv','JSON','Database'])
        self.ips = self.add(npyscreen.TitleText,name='Enter the list of IP addresses(separated by spaces):  ')
        self.ppath = self.add(npyscreen.TitleText, name = 'Enter path for dataset used in the program: ')
            
def myFunction(*args):
    global file_path,num_workers,format_type,model_type,iplist,program_path
    F = ApatheticML(name = "Choices for model and data:")
    F.edit()
    npyscreen.notify_wait('Thank you!')
    file_path = str(F.file.value)
    num_workers = F.splits.value
    format_type = F.format.value
    model_type = F.m[F.model.value[0]]
    iplist = F.ips.value.split()
    program_path = str(F.ppath.value)
    
    return "\nDeploying " + F.splits.value + ' workers to run ' + F.m[(F.model.value[0])] + ' on dataset in ' + str(F.file.value)+ F.ips.value + '\n'

def screen():
    print(npyscreen.wrapper_basic(myFunction)[0])
    return [file_path,num_workers,format_type,model_type,iplist,program_path]
   
#screen() 

#if __name__ == '__main__':
    #print(npyscreen.wrapper_basic(myFunction)[0])
    #print('test global \n',file_path)

