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
        envCheck: true
    },

    methods: {
        async extractAndTrain() {
            this.trainInProgress = true;
            await this.extractFeatures();
            await this.trainModel();
            this.trainInProgress = false;
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
                this.dataId = res.data.id
                this.featuresExtracted = true;
            })

        },

        async trainModel() {
            const data = {
                'data_id': this.dataId,
                'model_name': this.modelNameInput
            }    
            axios.post('/build', data);
        },

        predictInstance() {
            
        }
    }
  })