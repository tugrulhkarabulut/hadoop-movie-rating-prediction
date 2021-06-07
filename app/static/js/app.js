var app = new Vue({
    el: '#app',
    data: {
        activeComp: 'landing',
        datasetInput: "",
        tfIdfCheck: false,
        nGramCheck: false,
        exclamationCheck: false,
        featuresExtracted: false,
        dataId: '',
        modelNameInput: '',
        trainInProgress: false,
        envCheck: true,
        timeElapsed: 0
    },

    methods: {
        async extractAndTrain() {
            if (!this.featuresExtracted) {
                timer = this.startTimer();
                this.trainInProgress = true;
                await this.extractFeatures();
                //await this.trainModel();
                this.trainInProgress = false;
                window.clearInterval(timer);
            }
        },

        async extractFeatures() {

            const data = {
                'dataset_input': this.datasetInput,
                'feature_types': []
            }

            if (this.envCheck) {
                data.env = 'hadoop'
            } else {
                data.env = 'local'
            }

            
            if (this.tfIdfCheck) {
                data.feature_types.push('tf_idf')
            }

            if (this.nGramCheck) {
                data.feature_types.push('n_gram_count')
            }

            if (this.exclamationCheck) {
                data.feature_types.push('exclamation')
            }


            axios.post('/extract', data).then(res => {
                this.processName = res.data.process_name
                this.featuresExtracted = true;
            })

        },

        async trainModel() {
            const data = {
                'process_name': this.processName
            }    
            axios.post('/build', data);
        },

        startTimer() {
            return window.setInterval(() => {
                this.timeElapsed += 1 
            }, 1000)
        },

        predictInstance() {
            
        }
    }
  })