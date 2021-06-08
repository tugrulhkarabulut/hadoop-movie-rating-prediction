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
        processName: '',
        models: [],
        modelInput: "",
        summaryInput: "",
        reviewInput: "",
        unhelpfulCountInput: "",
        helpfulCountInput: "",
        hasSpoilersInput: false,
        predictionOutput: ""
    },

    methods: {
        onBuildModelClick() {
            this.activeComp = 'build-model';
        },

        onPredictClick() {
            this.activeComp = 'predict';
            this.getModels();
        },


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


        getModels() {
            axios.get('/get-models').then(res => {
                this.models = res.data.models
            })
        },

        predictInstance() {
            const data = {
                'model': this.modelInput,
                'spoiler': this.hasSpoilersInput,
                'summary_input': this.summaryInput,
                'review_input': this.reviewInput,
                'helpful_count': this.helpfulCountInput,
                'unhelpful_count': this.unhelpfulCountInput
            }

            axios.post('/predict', data).then(res => {
                //const probability = res.data.probability
                console.log(res.data);
                this.predictionOutput = res.data.prediction;
                
            })
        }
    }
  })