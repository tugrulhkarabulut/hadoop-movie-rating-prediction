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
        timeElapsed: 0,
        processNameInput: '',
        processName: ''
    },

    methods: {
        async extractAndTrain() {
            if (!this.featuresExtracted) {
                timer = this.startTimer();
                this.trainInProgress = true;
                await this.extractFeatures();
                await this.trainModel();
                this.trainInProgress = false;
                window.clearInterval(timer);
            }
        },

        async extractFeatures() {

            const data = {
                'dataset_input': this.datasetInput,
                'feature_types': [],
                'name': this.processNameInput
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


            await axios.post('/extract', data).then(res => {
                this.processName = res.data.process_name
                this.featuresExtracted = true;
            })

        },

        async trainModel() {
            const data = {
                'process_name': this.processName,
            }

            if (this.envCheck) {
                data.env = 'hadoop'
            } else {
                data.env = 'local'
            }

            const res = await axios.post('/build', data);

            console.log(res.data);
        },

        startTimer() {
            this.timeElapsed = 0;
            return window.setInterval(() => {
                this.timeElapsed += 1 
            }, 1000)
        },

        predictInstance() {
            
        }
    }
  })