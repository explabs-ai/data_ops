import h5py
import numpy as np


class HDF5():
    def __init__(self, filename):
        self.filename = filename

    def write_data(self, data, idfier, exists=False):
        fl = h5py.File(self.filename, 'a')

        if exists:
            dset = fl['/{}'.format(idfier)]
            incrementer = 1
            if len(data.shape) > 3:
                incrementer = data.shape[0]
            past_shape = dset.shape[0]
            fl['/{}'.format(idfier)].resize((dset.shape[0] +
											incrementer+1,)+(dset.shape[1:]))
            fl['/{}'.format(idfier)][past_shape+1:past_shape +
									incrementer+1, :] = data[:]
        else:
            incrementer = 0
            dset_shape = (1,)+data.shape
            max_shape = (None,)+data.shape
            if len(data.shape) > 3:
                dset_shape = data.shape
                incrementer = data.shape[0]
                max_shape = (None,)+data.shape[1:]

            fl.create_dataset(idfier, dset_shape,
							maxshape=max_shape, compression='gzip')

            fl['/{}'.format(idfier)][:incrementer] = data[:]

        fl.flush()
        fl.close()

    def write_attributes(self, idfier, keys, values):
        fl = h5py.File(self.filename, 'a')

        dset = fl['/{}'.format(idfier)]

        for i, key in enumerate(keys):
            value = values[i]
            if key in dset.attrs.keys():
                curr_val = eval(dset.attrs[key])
                value = str(curr_val + value)
            else:
                value = str(value)
            dset.attrs.__setitem__(key, value)

        fl.flush()
        fl.close()

    def read_data(self, idfier='', keys=[]):
        fl = h5py.File(self.filename, 'a')

        idfier = idfier if len(idfier) > 0 else list(fl.keys())[0]

        dataset = fl[idfier]
        meta = {}

        for key in keys:
            value = fl[idfier].attrs[key]
            value = eval(value)
            meta[key] = value

        dataset = np.array(dataset)
        fl.close()
        return dataset, meta