package vsphere

import (
	"fmt"
	"log"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"golang.org/x/net/context"
)

type file struct {
	datacenter      string
	datastore       string
	sourceFile      string
	destinationFile string
}

func resourceVSphereFile() *schema.Resource {
	return &schema.Resource{
		Create: resourceVSphereFileCreate,
		Read:   resourceVSphereFileRead,
		Update: resourceVSphereFileUpdate,
		Delete: resourceVSphereFileDelete,

		Schema: map[string]*schema.Schema{
			"datacenter": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},

			"datastore": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},

			"source_file": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"destination_file": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}

func resourceVSphereFileCreate(d *schema.ResourceData, meta interface{}) error {

	log.Printf("[DEBUG] creating file: %#v", d)
	client := meta.(*govmomi.Client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := file{}

	if v, ok := d.GetOk("datacenter"); ok {
		f.datacenter = v.(string)
	}

	if v, ok := d.GetOk("datastore"); ok {
		f.datastore = v.(string)
	} else {
		return fmt.Errorf("datastore argument is required")
	}

	if v, ok := d.GetOk("source_file"); ok {
		f.sourceFile = v.(string)
	} else {
		return fmt.Errorf("source_file argument is required")
	}

	if v, ok := d.GetOk("destination_file"); ok {
		f.destinationFile = v.(string)
	} else {
		return fmt.Errorf("destination_file argument is required")
	}

	err := createFile(ctx, client, &f)
	if err != nil {
		return err
	}

	d.SetId(fmt.Sprintf("[%v] %v/%v", f.datastore, f.datacenter, f.destinationFile))
	log.Printf("[INFO] Created file: %s", f.destinationFile)

	return resourceVSphereFileRead(d, meta)
}

func createFile(ctx context.Context, client *govmomi.Client, f *file) error {

	finder := find.NewFinder(client.Client, true)

	dc, err := finder.Datacenter(ctx, f.datacenter)
	if err != nil {
		return fmt.Errorf("error %s", err)
	}
	finder = finder.SetDatacenter(dc)

	ds, err := getDatastore(ctx, finder, f.datastore)
	if err != nil {
		return fmt.Errorf("error %s", err)
	}

	err = ds.UploadFile(ctx, f.sourceFile, f.destinationFile, nil)
	if err != nil {
		return fmt.Errorf("error %s", err)
	}
	return nil
}

func resourceVSphereFileRead(d *schema.ResourceData, meta interface{}) error {

	log.Printf("[DEBUG] reading file: %#v", d)
	client := meta.(*govmomi.Client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := file{}

	if v, ok := d.GetOk("datacenter"); ok {
		f.datacenter = v.(string)
	}

	if v, ok := d.GetOk("datastore"); ok {
		f.datastore = v.(string)
	} else {
		return fmt.Errorf("datastore argument is required")
	}

	if v, ok := d.GetOk("source_file"); ok {
		f.sourceFile = v.(string)
	} else {
		return fmt.Errorf("source_file argument is required")
	}

	if v, ok := d.GetOk("destination_file"); ok {
		f.destinationFile = v.(string)
	} else {
		return fmt.Errorf("destination_file argument is required")
	}


	finder := find.NewFinder(client.Client, true)

	dc, err := finder.Datacenter(ctx, f.datacenter)
	if err != nil {
		return fmt.Errorf("error %s", err)
	}
	finder = finder.SetDatacenter(dc)

	ds, err := getDatastore(ctx, finder, f.datastore)
	if err != nil {
		return fmt.Errorf("error %s", err)
	}

	_, err = ds.Stat(ctx, f.destinationFile)
	if err != nil {
		d.SetId("")
		return err
	}

	return nil
}

func resourceVSphereFileUpdate(d *schema.ResourceData, meta interface{}) error {

	log.Printf("[DEBUG] updating file: %#v", d)
	if d.HasChange("destination_file") {
		oldDestinationFile, newDestinationFile := d.GetChange("destination_file")
		f := file{}

		if v, ok := d.GetOk("datacenter"); ok {
			f.datacenter = v.(string)
		}

		if v, ok := d.GetOk("datastore"); ok {
			f.datastore = v.(string)
		} else {
			return fmt.Errorf("datastore argument is required")
		}

		if v, ok := d.GetOk("source_file"); ok {
			f.sourceFile = v.(string)
		} else {
			return fmt.Errorf("source_file argument is required")
		}

		if v, ok := d.GetOk("destination_file"); ok {
			f.destinationFile = v.(string)
		} else {
			return fmt.Errorf("destination_file argument is required")
		}

		client := meta.(*govmomi.Client)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dc, err := getDatacenter(client, f.datacenter)
		if err != nil {
			return err
		}

		finder := find.NewFinder(client.Client, true)
		finder = finder.SetDatacenter(dc)

		ds, err := getDatastore(ctx, finder, f.datastore)
		if err != nil {
			return fmt.Errorf("error uploading file: %s", err)
		}

		fm := object.NewFileManager(client.Client)
		task, err := fm.MoveDatastoreFile(ctx, ds.Path(oldDestinationFile.(string)), dc, ds.Path(newDestinationFile.(string)), dc, true)
		if err != nil {
			return err
		}

		_, err = task.WaitForResult(ctx, nil)
		if err != nil {
			return err
		}

	}

	return nil
}

func resourceVSphereFileDelete(d *schema.ResourceData, meta interface{}) error {

	log.Printf("[DEBUG] deleting file: %#v", d)
	client := meta.(*govmomi.Client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()


	f := file{}

	if v, ok := d.GetOk("datacenter"); ok {
		f.datacenter = v.(string)
	}

	if v, ok := d.GetOk("datastore"); ok {
		f.datastore = v.(string)
	} else {
		return fmt.Errorf("datastore argument is required")
	}

	if v, ok := d.GetOk("source_file"); ok {
		f.sourceFile = v.(string)
	} else {
		return fmt.Errorf("source_file argument is required")
	}

	if v, ok := d.GetOk("destination_file"); ok {
		f.destinationFile = v.(string)
	} else {
		return fmt.Errorf("destination_file argument is required")
	}



	err := deleteFile(ctx, client, &f)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func deleteFile(ctx context.Context, client *govmomi.Client, f *file) error {

	dc, err := getDatacenter(client, f.datacenter)
	if err != nil {
		return err
	}

	finder := find.NewFinder(client.Client, true)
	finder = finder.SetDatacenter(dc)

	ds, err := getDatastore(ctx, finder, f.datastore)
	if err != nil {
		return fmt.Errorf("error %s", err)
	}

	fm := object.NewFileManager(client.Client)
	task, err := fm.DeleteDatastoreFile(ctx, ds.Path(f.destinationFile), dc)
	if err != nil {
		return err
	}

	_, err = task.WaitForResult(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// getDatastore gets datastore object
func getDatastore(ctx context.Context, f *find.Finder, ds string ) (*object.Datastore, error) {

	if ds != "" {
		dso, err := f.Datastore(ctx, ds)
		return dso, err
	} else {
		dso, err := f.DefaultDatastore(ctx)
		return dso, err
	}
}
