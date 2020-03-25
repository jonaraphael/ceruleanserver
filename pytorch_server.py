def initialize(self, context):
    """
       Initialize the model and auxiliary attributes.
    """
    super(PyTorchImageClassifier, self).initialize(context)
    
    # Extract the model from checkpoint
    checkpoint = torch.load(self.checkpoint_file_path, map_location='cpu')
        self.model = checkpoint['model']

def preprocess(self, data):
    """
       Preprocess the data, transform or convert to tensor, etc
    """
        image = data[0].get("data")
        if image is None:
            image = data[0].get("body")

        my_preprocess = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], 
                                 std=[0.229, 0.224, 0.225])
        ])
        image = Image.open(io.BytesIO(image))
        image = my_preprocess(image)
        return image 

def inference(self, image):
    """
       Predict the class of image 
    """
    # Convert 2D image to 1D vector
    img = np.expand_dims(img, 0)
    img = torch.from_numpy(img)

    # Run forward pass
    self.model.eval()
    inputs = Variable(img).to(self.device)
    logits = self.model.forward(inputs)
    
    #Extract the top 5 species      
    ps = F.softmax(logits,dim=1)
    topk = ps.cpu().topk(5)
    probs, classes = (e.data.numpy().squeeze().tolist() for e in topk)

    # Formulate the result
    results = []
    for i in range(len(probs)):
       tmp = dict()
       tmp[self.mapping[str(classes[i])]] = probs[i]
       results.append(tmp)
    return [results]    


# $ ls /tmp/model-store
# index_to_name.json    model.pth    pytorch_service.py

# # Run the model-archiver on this folder to get the model archive
# $ model-archiver -f --model-name densenet161_pytorch --model-path /tmp/model-store --handler pytorch_service:handle --export-path /tmp

# # Verify that the model archive was created in the "export-path"
# $ ls /tmp
# densenet161_pytorch.mar


# $ mxnet-model-server --start --models densenet=https://s3.amazonaws.com/model-server/model_archive_1.0/examples/PyTorch+models/densenet/densenet161_pytorch.mar
