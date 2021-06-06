var app = new Vue({
    el: '#app',
    data: {
        activeComp: 'landing',
        message: 'Hello Vue!',
        datasetInput: "",
        tfIdfCheck: false,
        nGramCheck: false,
        exclamationCheck: false,
        featuresExtracted: false,
        dataId: '',
        modelNameInput: ''
    },

    methods: {
        extractFeatures() {
            const data = {
                'datasetInput': this.datasetInput,
                'tf_idf': this.tfIdfCheck,
                'n_gram_count': this.nGramCheck,
                'exclamation': this.exclamationCheck
            }


            axios.post('/extract', data).then(res => {
                this.dataId = res.data.id
                this.featuresExtracted = true;
            })

        },

        trainModel() {
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