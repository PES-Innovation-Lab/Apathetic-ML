
import npyscreen
num_workers=format_type=model_type=program_path=hyperparams=None

class First(npyscreen.Form):
    def create(self):
        self.m = ['Linear Regression','Logistic Regression','K-Means Clustering','Random Forest Classification','Neural Networks Classification']
        self.f=['.csv','.json','Database']
        self.splits  = self.add(npyscreen.TitleText,name='No. of workers:')
        self.model = self.add(npyscreen.TitleSelectOne, scroll_exit=True, max_height=5, name='Model:', values = self.m)
        self.format = self.add(npyscreen.TitleSelectOne, scroll_exit=True,max_height = 5,name='Data format: ',values=self.f)
    def afterEditing(self):
        global num_workers,format_type,model_type
        num_workers = int(self.splits.value)
        format_type = self.f[self.format.value[0]]
        model_type = self.m[self.model.value[0]]
        self.editing=True
        self.parentApp.addForm("SECOND", Second, name="Apathetic ML")
        self.parentApp.switchForm('SECOND')

class Second(npyscreen.Form):
    def create(self):
        global model_type
        self.ppath = self.add(npyscreen.TitleFilename, name = 'Path of dataset wrt program (TAB for autocomplete):')
        if model_type in ['Linear Regression','Logistic Regression']:
            self.batch_size=self.add(npyscreen.TitleText,name='Batch Size:')
            self.n_iters=self.add(npyscreen.TitleText,name='No. of iterations:')
            self.learning_rate=self.add(npyscreen.TitleText,name='Learning Rate:')
        elif model_type=='K-Means Clustering':
            self.n_iters=self.add(npyscreen.TitleText,name='No. of iterations:')
            self.k=self.add(npyscreen.TitleText,name='K:')
        elif model_type=='Random Forest Classification':
            self.n_trees=self.add(npyscreen.TitleText,name='No. of Decision Trees:')
        elif model_type=='Neural Networks Classification':
            self.steps=self.add(npyscreen.TitleText,name='No. of steps:')
    def afterEditing(self):
        global format_type,model_type,program_path,hyperparams
        print(format_type,model_type,program_path,hyperparams)
        program_path = self.ppath.value
        if format_type not in program_path:
            npyscreen.notify_confirm('Please enter valid dataset format/path',title="Apathetic ML")
            self.editing=True
            self.parentApp.switchFormPrevious('MAIN')
        else:
            if model_type in ['Linear Regression','Logistic Regression']:
                hyperparams=[int(self.batch_size.value),int(self.n_iters.value),float(self.learning_rate.value)]
            elif model_type=='K-Means Clustering':
                hyperparams=[int(self.n_iters.value),int(self.k.value)]
            elif model_type=='Random Forest Classification':
                hyperparams=[int(self.n_trees.value)]
            elif model_type=='Neural Networks Classification':
                hyperparams=[int(self.steps.value)]
            self.parentApp.setNextForm(None)

class AML(npyscreen.NPSAppManaged):
    def onStart(self):
        self.addForm("MAIN", First, name="Apathetic ML")


def screen():
    global num_workers,format_type,model_type,program_path,hyperparams
    myTUI=AML()
    myTUI.run()
    npyscreen.notify_wait('Thank you!',title="Apathetic ML")
    return [num_workers,format_type,model_type,program_path,hyperparams]

#screen() 

